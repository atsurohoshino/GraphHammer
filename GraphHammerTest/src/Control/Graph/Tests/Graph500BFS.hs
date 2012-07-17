-- |Graph500BFS
--
-- This is an implementation of Graph500 BFS in our EDSL.
--
-- Copyright (C) 2012 Parallel Scientific

{-# LANGUAGE DeriveDataTypeable #-}

module Control.Graph.Tests.Graph500BFS where

import Control.Monad
import Data.Char
import Data.List

import LanGra

-------------------------------------------------------------------------------
-- The analysis.

data G500EdgeCount = G500EdgeCount
        deriving Typeable

graph500BFS :: Index -> AnM ()
graph500BFS startIndex = do
        -- clear count.
        putAnalysisResult G500EdgeCount (cst 0) (cst 0)

        start <- localValue (cst startIndex)

        currentSet <- localValue setEmpty
        front <- localValue $ setSingle start

        -- While set is grown at current step...
        anWhile (notV $ isNullSet front) $ do
                currentSet $= setUnion currentSet front
                incr <- localValue setEmpty
                -- for each vertices that are in current set of vertices.
                onSetVertices front $ \v -> do
                        -- compute neighbors that weren't seen already.
                        neighbors <- vertexNeighbors v
                        neighbors $= setDifference neighbors currentSet
                        -- and add them into current set.
                        incr $= setUnion incr neighbors
                        incrementAnalysisResult G500EdgeCount (cst 0) (cst 1)
                -- verification code.
--              reportSet incr
                front $= incr
        -- special code to report the set.
--      reportSet currentSet
