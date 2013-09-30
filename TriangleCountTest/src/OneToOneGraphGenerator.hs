-- |OneToOneGraphGenerator
-- Copyright : (C) 2013 Parallel Scientific Labs, LLC.
-- License   : GPLv2
--
-- A test data generator - it generates a graph that connect each
-- node to any each other node.
--
-- There is an option on how to generate edge list. It is possible to
-- omit doubles, as our graph is undirected one.
--

module OneToOneGraphGenerator(
	  oneToOneGraph
) where

import GraphHammer

oneToOneGraph :: Bool -> Int -> [(Index,Index)]
oneToOneGraph undirected n = edges
	where
		range = [0..n-1]
		edges = [(fromIntegral i,fromIntegral j)
			| i <- range, j <- range, undirected || i < j]
