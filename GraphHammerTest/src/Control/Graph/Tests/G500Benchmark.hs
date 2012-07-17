-- |Graph500BFS
--
-- Graph500 BFS benchmark main module.
--
-- Copyright (C) 2012 Parallel Scientific

module Control.Graph.Tests.G500Benchmark (runTest) where

import Control.Monad
import Data.Array.Unboxed
import Data.Time
import System.Environment
import System.Exit
import System.IO
import System.Random

import G500
import G500.Read
import qualified LanGra
import Control.Graph.Tests.Graph500BFS
import Control.Graph.Interface
import Control.Graph.HiPerQ

-------------------------------------------------------------------------------
-- Time everything.

timeAction :: String -> IO r -> IO r
timeAction msg act = do
        start <- getCurrentTime 
        r <- act
        end <- getCurrentTime
        putStrLn $ "Timed "++show msg
        putStrLn $ "Elapsed time: "++show (diffUTCTime end start)
        return r

-------------------------------------------------------------------------------
-- Parse command line and load graph.

loadGraph :: Interface a => QHandle a -> String -> IO ()
loadGraph qh fn = do
        h <- openBinaryFile fn ReadMode
        reader <- mkGraph500Reader h 0x100000
        let readGraph = do
                edges <- reader
                case edges of
                        Just edges -> do
                                addEdges qh edges
                                readGraph
                        Nothing -> return ()
        readGraph

-------------------------------------------------------------------------------
-- Run BFS analysis and display count of edges visited.

runBFSSearch :: Interface a => QHandle a -> Int -> IO Int
runBFSSearch qh i = do
        maxVertexIndexPow2 <- liftM (mkPow2 1) $ getMaxVertexIndex qh
        let gen = snd $ next $ mkStdGen (i*65537+24101971)
            (randomVertex,_) = randomIndex (maxVertexIndexPow2-1) gen
        putStrLn $ "Max index (rounded to higher power of 2) "++show maxVertexIndexPow2++", random vertex "++show randomVertex
        --putStrLn "runBFSSearch is not yet done."
        runAnalysis qh $ graph500BFS randomVertex
        n <- getAnalysisResult qh G500EdgeCount 0
        putStrLn $ "Count of processed edges "++show n
        return $ fromIntegral n
        where
                mkPow2 n x
                        | n < x = mkPow2 (2*n) x
                        | otherwise = n

-------------------------------------------------------------------------------
-- Main driver.

runTest :: Interface a => a -> FilePath -> IO [Int]
runTest myBackend filename =
  do qh <- newQHandle myBackend
     timeAction "loading a graph." $ loadGraph qh filename
     res <- forM [1..64] $ \i -> do
               timeAction ("BFS iteration "++show i) $ runBFSSearch qh i     
     closeQHandle qh
     return res

main :: IO ()
main = 
  do putStrLn "Graph500 benchmark"
     args <- getArgs
     case args of
         [fn] -> runTest simpleBackend fn
                    >> return ()
         _ -> putStrLn "Please give g500 data file name on commnad line" 
                  >> exitFailure
