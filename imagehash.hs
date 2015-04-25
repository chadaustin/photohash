{-# LANGUAGE OverloadedStrings #-}

import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.TMQueue
import Control.Monad
import Data.ByteString.Char8 (unpack)
import Data.Char
import Data.IORef (newIORef, modifyIORef, readIORef)
import Data.List
import Debug.Trace
import GHC.Conc
import System.Environment (getArgs)
import System.FilePath
import System.IO
import System.Process
import qualified Crypto.Hash.SHA1 as SHA1
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as BS
import qualified System.Directory as Directory
import qualified System.Process as Process

-- Generic TMQueue utilities

processQueue queue action = do
  entry <- atomically $ readTMQueue queue
  case entry of
    Nothing ->
      return ()
    Just entry -> do
      action entry
      processQueue queue action

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
  forkIO loop
  return output
  
--

walk :: FilePath -> ((FilePath, [FilePath], [FilePath]) -> IO ()) -> IO ()
walk root action = do
  entries <- Directory.getDirectoryContents root
  directories <- newIORef []
  files <- newIORef []
  
  forM_ entries $ \entry -> do
    let fullPath = combine root entry
    isFile <- Directory.doesFileExist fullPath
    if isFile then
      modifyIORef files (entry:)
     else do
      when (entry /= "." && entry /= "..") $ do
        isDirectory <- Directory.doesDirectoryExist fullPath
        when isDirectory $ modifyIORef directories (entry:)

  ds <- readIORef directories
  fs <- readIORef files
  action (root, sort ds, sort fs)

  forM_ ds $ \dir ->
    walk (combine root dir) action

-- compatibility with os.walk semantics
walkDirectory :: FilePath -> TMQueue FilePath -> IO ()
walkDirectory here fileQueue = do
  walk here $ \(dirpath, _dirnames, filenames) -> do
    let fns = map (combine dirpath) filenames
    forM_ fns $ \fn ->
      atomically $ writeTMQueue fileQueue fn

type Hash = BS.ByteString
data HashResult = HashResult FilePath Hash
type JobQueue = TQueue (IO ())
type HashFunction = FilePath -> IO (MVar HashResult)

forkJobRunnerOnQueue queue = do
  forkIO $ forever $ do
    action <- atomically $ readTQueue queue
    action

runOnQueue :: JobQueue -> IO a -> IO (MVar a)
runOnQueue queue action = do
  result <- newEmptyMVar
  atomically $ writeTQueue queue $ do
    r <- action
    putMVar result r
  return result

sha1Hash :: JobQueue -> HashFunction
sha1Hash diskQueue path = runOnQueue diskQueue $ do
  contents <- BS.readFile path
  return $ HashResult path $ B16.encode $ SHA1.hash contents

pipe' :: StdStream -> [[String]] -> IO Handle
pipe' stdin [(cmd:args)] = do
  (_, (Just stdout), _, handle) <- createProcess (proc cmd args){
    std_in = stdin,
    std_out = CreatePipe }
  return stdout
pipe' stdin ((cmd:args):rest) = do
  (_, (Just stdout), _, handle) <- createProcess (proc cmd args){
    std_in = stdin,
    std_out = CreatePipe }
  pipe' (UseHandle stdout) rest

pipe :: [[String]] -> IO Handle
pipe commands = pipe' Inherit commands

stripSuffix n xs = take (max 0 (length xs - n)) xs

hashPipe :: [[String]] -> IO BS.ByteString
hashPipe commands = do
  stdout <- pipe $ commands ++ [["sha1sum"]]
  line <- hGetLine stdout

  let valid = isSuffixOf " *-" line || isSuffixOf "  -" line
  when (not valid) $ fail $ "hash line does not end with *-: " ++ line
  let hash = stripSuffix 3 line
  return $ BS.pack hash

djpegOptions = ["-dct", "int", "-dither", "none", "-nosmooth"]

imageHash :: JobQueue -> HashFunction
imageHash queue path = do
  outputMVars <- mapM (runOnQueue queue . hashPipe) [
     [ ["djpeg"] ++ djpegOptions ++ ["-bmp", path] ],
     [ ["jpegtran", "-rotate", "90", path],
       ["djpeg"] ++ djpegOptions ++ ["-bmp"] ],
     [ ["jpegtran", "-rotate", "180", path],
       ["djpeg"] ++ djpegOptions ++ ["-bmp"] ],
     [ ["jpegtran", "-rotate", "270", path],
       ["djpeg"] ++ djpegOptions ++ ["-bmp"] ] ]

  result <- newEmptyMVar
  forkIO $ do
    pipes <- mapM takeMVar outputMVars
    putMVar result $ HashResult path $ minimum pipes
  return result

getHashFunction :: (JobQueue, JobQueue) -> FilePath -> HashFunction
getHashFunction (cpuQueue, diskQueue) path =
  case (map toLower extension) of
    ".jpg" -> imageHash cpuQueue
    ".jpeg" -> imageHash cpuQueue
    _ -> sha1Hash diskQueue
  where extension = takeExtension path

hashFile :: (JobQueue, JobQueue) -> FilePath -> IO (MVar HashResult)
hashFile queues path = do
  let hasher = getHashFunction queues path
  hasher path

readFileList paths = do
  fileQueue <- newTMQueueIO
  forkIO $ do
    forM_ paths $ \path ->
      walkDirectory path fileQueue
    atomically $ closeTMQueue fileQueue
  return fileQueue

printHashResult result = do
  (HashResult path hash) <- takeMVar result
  putStrLn $ (unpack hash) ++ " *" ++ path

newRunnerQueue concurrency = do
  queue <- newTQueueIO
  replicateM_ concurrency $ forkJobRunnerOnQueue queue
  return queue

main :: IO ()
main = do
  -- TODO: measure SSD/spinny/NAS optimal concurrency
  let diskConcurrencyCount = 1

  --let cpuConcurrencyCount = 1
  cpuConcurrencyCount <- getNumProcessors
  setNumCapabilities cpuConcurrencyCount

  hSetBuffering stdout NoBuffering

  cpuQueue <- newRunnerQueue cpuConcurrencyCount
  diskQueue <- newRunnerQueue diskConcurrencyCount

  args <- getArgs
  paths <- case args of
    [] -> do
      here <- Directory.getCurrentDirectory
      return [here]
    _ -> return args
    
  fileQueue <- readFileList paths
  resultQueue <- pipeline fileQueue $ hashFile (cpuQueue, diskQueue)
  processQueue resultQueue printHashResult
