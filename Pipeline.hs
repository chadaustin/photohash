{-# LANGUAGE LambdaCase #-}

module Pipeline
       ( JobRunner
       , newJobRunner
       , stopJobRunner
       , queueJob
       ) where

import Control.Concurrent (MVar, newEmptyMVar, putMVar)
import Control.Concurrent.Async (Async, async, wait)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TMQueue (TMQueue, newTMQueueIO, readTMQueue, writeTMQueue, closeTMQueue)
import Control.Monad (forever, forM_, replicateM)

data JobRunner = JobRunner !(TMQueue (IO ())) ![Async ()]

newJobRunner :: Int -> IO JobRunner
newJobRunner concurrency = do
  queue <- newTMQueueIO
  runners <- replicateM concurrency $ do
    async $ forever $ do
      let loop = do
            (atomically $ readTMQueue queue) >>= \case
              Just action -> action >> loop
              Nothing -> return ()
      loop
  return $ JobRunner queue runners

stopJobRunner :: JobRunner -> IO ()
stopJobRunner (JobRunner queue runners) = do
  atomically $ closeTMQueue queue
  forM_ runners wait

queueJob :: JobRunner -> IO a -> IO (MVar a)
queueJob (JobRunner queue _) action = do
  result <- newEmptyMVar
  atomically $ writeTMQueue queue $ do
    -- TODO: exception safety
    r <- action
    putMVar result r
  return result
