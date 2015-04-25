{-# LANGUAGE OverloadedStrings #-}

import Control.Monad
import qualified Crypto.Hash.SHA1 as SHA1
import qualified Data.ByteString.Char8 as BS
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.TMQueue
import System.FilePath
import Data.ByteString.Char8 (unpack)
import System.Process
import System.IO
import Data.List
import Data.Char
import Debug.Trace
import qualified System.Directory as Directory
import qualified Data.ByteString.Base16 as B16
import qualified System.Process as Process
import GHC.Conc

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

walkDirectory :: FilePath -> TMQueue FilePath -> IO ()
walkDirectory here fileQueue = do
  entries <- Directory.getDirectoryContents here
  forM_ (sort entries) $ \entry -> do
    let fullPath = combine here entry
    isDirectory <- Directory.doesDirectoryExist fullPath
    isFile <- Directory.doesFileExist fullPath
    when (isDirectory && entry /= "." && entry /= "..") $ do
      walkDirectory fullPath fileQueue
    when isFile $ do
      atomically $ writeTMQueue fileQueue fullPath

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

{-

djpeg $options -bmp $fn | sha1sum
jpegtran -rotate 90 $fn | djpeg $options -bmp | sha1sum
jpegtran -rotate 180 $fn | djpeg $options -bmp | sha1sum
jpegtran -rotate 270 $fn | djpeg $options -bmp | sha1sum

-}

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

readFileList here = do
  fileQueue <- newTMQueueIO
  forkIO $ do
    walkDirectory here fileQueue
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

  -- here <- Directory.getCurrentDirectory
  fileQueue <- readFileList "."
  resultQueue <- pipeline fileQueue $ hashFile (cpuQueue, diskQueue)
  processQueue resultQueue printHashResult
