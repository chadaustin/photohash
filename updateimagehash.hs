{-# LANGUAGE ScopedTypeVariables #-}

import qualified Data.Map as Map
import Data.ByteString (ByteString)
import qualified Data.List as List
import qualified Data.ByteString.Char8 as BSC
import Data.IORef
import Control.Exception (tryJust)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Concurrent.STM.TMQueue
import Control.Monad
import Data.Maybe
import System.IO
import System.IO.Error
import System.FilePath
import System.Directory
import System.Directory.PathWalk (pathWalk)

import ImageHash

excludedFiles :: [String]
excludedFiles = [
  "Picasa.ini",
  "Thumbs.db",
  ".DS_Store",
  "imagehash.txt" ]

parse :: String -> Maybe (FilePath, ByteString)
parse line =
  case words line of
    [h, p] -> if "*./" `List.isPrefixOf` p then Just (drop 3 p, BSC.pack h) else Nothing
    _ -> Nothing

readHashes :: FilePath -> IO [(FilePath, ByteString)]
readHashes path = do
  result <- tryJust (guard . isDoesNotExistError) $ readFile path
  return $ case result of
    Left _ -> []
    Right contents -> catMaybes $ map parse $ lines contents

writeHashes :: FilePath -> [(FilePath, ByteString)] -> IO ()
writeHashes path hashes = do
  withFile (path ++ ".new") WriteMode $ \handle -> do
    forM_ hashes $ \(path', hash) ->
      hPutStrLn handle $ (BSC.unpack hash) ++ " *./" ++ path'

  renameFile (path ++ ".new") path

updateHashes :: Hasher -> FilePath -> Map.Map FilePath ByteString -> IO (Map.Map FilePath ByteString)
updateHashes hasher root original = do
  fileQueue <- newTMQueueIO
  resultQueue <- pipeline fileQueue hasher

  hashes <- newIORef Map.empty

  putStr "scanning: "

  -- find unknown hashes and insert into job queue
  pathWalk root $ \dirpath _dirnames filenames -> do
    putChar '.'
    forM_ (map (normalise . combine dirpath) (filter (`notElem` excludedFiles) filenames)) $ \fullPath -> do
      case Map.lookup fullPath original of
        Nothing -> atomically $ writeTMQueue fileQueue fullPath
        Just hash -> modifyIORef hashes $ Map.insert fullPath hash
  atomically $ closeTMQueue fileQueue
  putChar '\n'

  -- remove hashes not in map
  h <- readIORef hashes
  forM_ (Map.toList $ Map.difference original h) $ \(path, _hash) -> do
    putStrLn $ "removed: " ++ show path
  
  -- consume results and update hash map
  newHashes <- newIORef original
  processQueue resultQueue $ \mv -> do
    HashResult path hash <- readMVar mv
    putStrLn $ "added: " ++ path ++ " " ++ BSC.unpack hash
    modifyIORef newHashes $ Map.insert path hash
        
  readIORef newHashes

main :: IO ()
main = do
  let hashfile = "imagehash.txt"
  hasher <- makeHasher

  originalHashes <- fmap Map.fromList $ readHashes hashfile

  hashes <- updateHashes hasher "." originalHashes

  writeHashes hashfile $ Map.toList hashes
