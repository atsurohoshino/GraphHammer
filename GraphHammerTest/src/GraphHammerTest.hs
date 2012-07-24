{-# LANGUAGE GADTs, Rank2Types,TypeFamilies #-}
module Main where

import Control.Monad (forM_)

import Test.HUnit

import G500
import G500.GenerateFile
import Control.Graph.Interface
import Control.Graph.HiPerQ
import qualified Control.Graph.Tests.G500Benchmark as G500Benchmark (runTest)
import qualified Control.Graph.Tests.VertexDegree as VertexDegree (runTest)
import qualified Control.Graph.Tests.TriangleCount as TriangleCount (runTest)

data Backend where
  Backend :: Interface a => a -> Backend

backends :: [Backend]
backends = [] -- other backends to test, excluding the reference backend

makeTests :: (Eq b, Show b) => FilePath -> String -> (forall a.Interface a => a -> FilePath -> IO b) -> Test
makeTests filename label f =
    let myTests = map (\(Backend interface) -> 
                         (f interface, versionString interface)) backends
        testName ifname = "In test " ++ label ++ ", mismatch between reference and " ++ ifname
     in TestLabel label $ TestCase $    
                      do expected <- f referenceBackend filename
                         forM_ myTests $ \(assertion, ifname) ->
                                         do thisResult <- assertion filename
                                            assertEqual (testName ifname)
                                                  expected thisResult
      where referenceBackend = simpleBackend

graphDataFile :: FilePath
graphDataFile = "g500_testdata"

setupTests :: IO Test
setupTests = do generateWriteFile graphDataFile Graph500 10 16
                return $ TestList [makeTests graphDataFile "VertexDegree" VertexDegree.runTest,
                                   makeTests graphDataFile "G500Benchmark" G500Benchmark.runTest,
                                   makeTests {-ignored-}undefined "TriangleCount" TriangleCount.runTest]

main = 
   do tests <- setupTests
      runTestTT tests

