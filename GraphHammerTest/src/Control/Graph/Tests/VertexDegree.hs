-- |STINGER.VertexDegree
--
-- Vertex degree analysis.

{-# LANGUAGE TypeFamilies, TypeOperators, FlexibleInstances, MultiParamTypeClasses #-}
{-# LANGUAGE FlexibleContexts, DeriveDataTypeable #-}

module Control.Graph.Tests.VertexDegree(runTest) where

import Control.Monad
import Data.Char
import Data.List
import System.Environment
import System.Exit
import System.IO
import Text.Printf

import G500
import G500.Read
import LanGra
import Control.Graph.Tests.Graph500BFS
import Control.Graph.Interface
import Control.Graph.HiPerQ

------------------------------------------------------------------------------------
-- Simple vertex degree streaming analysis.

data VertexDegree = VertexDegree
        deriving Typeable

vertexDegree :: Value Comp Bool -> Value Comp Index -> Value Comp Index -> AnM ()
vertexDegree added fromV toV = do
        incr <- localValue (cst 1)
        anWhen (notV added) (incr $= cst (-1))
        incrementAnalysisResult VertexDegree fromV incr
        incrementAnalysisResult VertexDegree toV incr

------------------------------------------------------------------------------------
-- Load graph. 

loadGraph :: Interface a => QHandle a -> FilePath -> IO ()
loadGraph qh fp = do
        h <- openBinaryFile fp ReadMode
        reader <- mkGraph500Reader h 0x100000
        let readGraph = do
                edges <- reader
                case edges of
                        Just edges -> do
                                addAnalyzeEdges qh vertexDegree edges
                                readGraph
                        Nothing -> return ()
        readGraph

------------------------------------------------------------------------------------
-- Running the test.


runTest :: Interface a => a -> FilePath -> IO [Int]
runTest myBackend fp =
  do    qh <- newQHandle myBackend
        loadGraph qh fp
        i <- getMaxVertexIndex qh
        res <- forM [0..i-1] $ \n -> do
                r <- Control.Graph.Interface.getAnalysisResult qh VertexDegree i
                putStrLn $ printf "%6d: %d" n r
                return $ fromIntegral r
        closeQHandle qh
        return res


main :: IO ()
main = 
  do putStrLn "VertexDegree Test"
     args <- getArgs
     case args of
         [fn] -> runTest simpleBackend fn 
                  >> return ()
         _ -> putStrLn "Please give g500 data file name on commnad line" 
                  >> exitFailure

