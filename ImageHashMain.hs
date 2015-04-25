import System.IO
import qualified System.Directory as Directory
import System.Environment

import ImageHash

main :: IO ()
main = do
  hSetBuffering stdout NoBuffering

  args <- getArgs
  paths <- case args of
    [] -> do
      here <- Directory.getCurrentDirectory
      return [here]
    _ -> return args
    
  fileQueue <- readFileList paths
  hasher <- makeHasher
  resultQueue <- pipeline fileQueue hasher
  processQueue resultQueue printHashResult
