{-# LANGUAGE OverloadedStrings, LambdaCase #-}

module ImageHash
       ( Hasher
       , HashResult(..)
       , makeHasher
       , readFileList
       , pipeline
       , processQueue
       , processResultQueue
       , printHash
       ) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.TMQueue
import Control.Monad
import Data.ByteString.Char8 (unpack)
import Data.Char
import Data.List
import GHC.Conc
import System.FilePath
import System.IO (Handle, hGetLine)
import System.Process
import qualified Crypto.Hash.SHA1 as SHA1
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as BS
import qualified System.Directory.PathWalk as PathWalk

import Pipeline

-- Generic TMQueue utilities

processQueue :: TMQueue a -> (a -> IO ()) -> IO ()
processQueue queue action = do
  atomically (readTMQueue queue) >>= \case
    Nothing ->
      return ()
    Just entry -> do
      action entry
      processQueue queue action

processResultQueue :: TMQueue (MVar a) -> (a -> IO ()) -> IO ()
processResultQueue queue action = do
  atomically (readTMQueue queue) >>= \case
    Nothing ->
      return ()
    Just entry -> do
      entry' <- takeMVar entry
      action entry'
      processResultQueue queue action

pipeline :: TMQueue a -> (a -> IO b) -> IO (TMQueue b)
pipeline input transform = do
  output <- newTMQueueIO
  let loop = do
      entry <- atomically $ readTMQueue input
      case entry of
        Nothing -> do
          atomically $ closeTMQueue output
        Just x -> do
          o <- transform x
          atomically $ writeTMQueue output o
          loop
  _ <- forkIO loop
  return output
  
--

type Hash = BS.ByteString
data HashResult = HashResult FilePath Hash
type HashFunction = FilePath -> IO (MVar HashResult)

sha1Hash :: JobRunner -> HashFunction
sha1Hash diskQueue path = queueJob diskQueue $ do
  contents <- BS.readFile path
  return $ HashResult path $ B16.encode $ SHA1.hash contents

pipe' :: StdStream -> [[String]] -> IO Handle
pipe' stdin [(cmd:args)] = do
  (_, (Just stdout), _, _handle) <- createProcess (proc cmd args){
    std_in = stdin,
    std_out = CreatePipe }
  return stdout
pipe' stdin ((cmd:args):rest) = do
  (_, (Just stdout), _, _handle) <- createProcess (proc cmd args){
    std_in = stdin,
    std_out = CreatePipe }
  pipe' (UseHandle stdout) rest
pipe' _ _ = fail "Not enough arguments"

pipe :: [[String]] -> IO Handle
pipe commands = pipe' Inherit commands

stripSuffix :: Int -> [a] -> [a]
stripSuffix n xs = take (max 0 (length xs - n)) xs

hashPipe :: [[String]] -> IO BS.ByteString
hashPipe commands = do
  stdout <- pipe $ commands ++ [["sha1sum"]]
  line <- hGetLine stdout

  let valid = isSuffixOf " *-" line || isSuffixOf "  -" line
  when (not valid) $ fail $ "hash line does not end with *-: " ++ line
  let hash = stripSuffix 3 line
  return $ BS.pack hash

djpegOptions :: [String]
djpegOptions = ["-dct", "int", "-dither", "none", "-nosmooth"]

imageHash :: JobRunner -> HashFunction
imageHash queue path = do
  outputMVars <- mapM (queueJob queue . hashPipe) [
     [ ["djpeg"] ++ djpegOptions ++ ["-bmp", path] ],
     [ ["jpegtran", "-rotate", "90", path],
       ["djpeg"] ++ djpegOptions ++ ["-bmp"] ],
     [ ["jpegtran", "-rotate", "180", path],
       ["djpeg"] ++ djpegOptions ++ ["-bmp"] ],
     [ ["jpegtran", "-rotate", "270", path],
       ["djpeg"] ++ djpegOptions ++ ["-bmp"] ] ]

  result <- newEmptyMVar
  _ <- forkIO $ do
    pipes <- mapM takeMVar outputMVars
    putMVar result $ HashResult path $ minimum pipes
  return result

getHashFunction :: (JobRunner, JobRunner) -> FilePath -> HashFunction
getHashFunction (cpuQueue, diskQueue) path =
  case (map toLower extension) of
    ".jpg" -> imageHash cpuQueue
    ".jpeg" -> imageHash cpuQueue
    _ -> sha1Hash diskQueue
  where extension = takeExtension path

hashFile :: (JobRunner, JobRunner) -> FilePath -> IO (MVar HashResult)
hashFile queues path = do
  let hasher = getHashFunction queues path
  hasher path

readFileList :: [FilePath] -> IO (TMQueue FilePath)
readFileList paths = do
  fileQueue <- newTMQueueIO
  _ <- forkIO $ do
    forM_ paths $ \path -> do
      PathWalk.pathWalk path $ \dir _subdirs files -> do
        let fns = map (combine dir) files
        forM_ fns $ \fn ->
          atomically $ writeTMQueue fileQueue fn
        
    atomically $ closeTMQueue fileQueue
  return fileQueue

printHash :: HashResult -> IO ()
printHash (HashResult path hash) = do
  putStrLn $ (unpack hash) ++ " *" ++ path
  
type Hasher = FilePath -> IO (MVar HashResult)

makeHasher :: IO Hasher
makeHasher = do
  -- TODO: measure SSD/spinny/NAS optimal concurrency
  let diskConcurrencyCount = 1

  --let cpuConcurrencyCount = 1
  cpuConcurrencyCount <- getNumProcessors
  setNumCapabilities cpuConcurrencyCount

  cpuQueue <- newJobRunner cpuConcurrencyCount
  diskQueue <- newJobRunner diskConcurrencyCount
  
  return $ hashFile (cpuQueue, diskQueue)
