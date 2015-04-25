{-# LANGUAGE ScopedTypeVariables #-}

import qualified Data.Map as Map
import Data.ByteString (ByteString)
import qualified Data.List as List
import qualified Data.ByteString.Char8 as BSC
import Data.IORef
import Control.Exception
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Concurrent.STM.TMQueue
import Control.Monad
import Data.Maybe
import System.IO
import System.IO.Error
import System.FilePath
import System.Directory

import ImageHash

excludedFiles = [
  "Picasa.ini",
  "Thumbs.db",
  ".DS_Store",
  "imagehash.txt" ]

parse :: String -> Maybe (FilePath, ByteString)
parse line =
  case words line of
    [h, p] -> if "*" `List.isPrefixOf` p then Just (drop 1 p, BSC.pack h) else Nothing
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
    forM_ hashes $ \(path, hash) ->
      hPutStrLn handle $ (BSC.unpack hash) ++ " *./" ++ path

  renameFile (path ++ ".new") path

updateHashes :: Hasher -> FilePath -> Map.Map FilePath ByteString -> IO (Map.Map FilePath ByteString)
updateHashes hasher root original = do
  fileQueue <- newTMQueueIO
  resultQueue <- pipeline fileQueue hasher

  hashes <- newIORef Map.empty

  putStrLn "scanning"

  -- find unknown hashes and insert into job queue
  walk root $ \(dirpath, dirnames, filenames) -> do
    forM_ (map (combine dirpath) (filter (`notElem` excludedFiles) filenames)) $ \fullPath -> do
      putChar '.'
      case Map.lookup fullPath original of
        Nothing -> atomically $ writeTMQueue fileQueue fullPath
        Just hash -> modifyIORef hashes $ Map.insert fullPath hash
  atomically $ closeTMQueue fileQueue
  putChar '\n'

  -- remove hashes not in map
  h <- readIORef hashes
  forM (Map.toList $ Map.difference original h) $ \path -> do
    putStrLn $ "removed: " ++ show path
  
  -- consume results and update hash map
  hashes <- newIORef original
  processQueue resultQueue $ \mv -> do
    HashResult path hash <- readMVar mv
    putStrLn $ "added: " ++ show path
    modifyIORef hashes $ Map.insert path hash
        
  readIORef hashes

main :: IO ()
main = do
  let hashfile = "imagehash.txt"
  hasher <- makeHasher

  originalHashes <- fmap Map.fromList $ readHashes hashfile

  hashes <- updateHashes hasher "." originalHashes

  writeHashes hashfile $ Map.toList hashes
