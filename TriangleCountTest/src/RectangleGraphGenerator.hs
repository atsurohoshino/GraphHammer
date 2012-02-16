-- |SquareGraphGenerator
--
-- A test data generator - it generates a simple rectangular planar graph
--

module RectangleGraphGenerator(
	  encodeIndex
	, decodeIndex
	, regularRectangleGraph
) where

import STINGER

encodeIndex :: Int -> (Int, Int) -> Index
encodeIndex nCols (i,j) = fromIntegral $ i*nCols+j

decodeIndex :: Int -> Index -> (Int, Int)
decodeIndex nCols ij = (div (fromIntegral ij) nCols, mod (fromIntegral ij) nCols)

-- |Make a list of edges for graph with nRows*nCols nodes.
-- Nodes are points in (0..nRows-1,0..nCols-1) space.
-- Edges connect nodes in rows, nodes in columns and
-- diagonals (i,j) -- (i+1, j+1).
--
-- This way internal nodes will have 6 triangles, side nodes three triangles
-- and corner nodes only two triangles.
--
regularRectangleGraph :: Int -> Int -> [(Index, Index)]
regularRectangleGraph nRows nCols = map encodeEdge $ concat $ rows ++ columns ++ diags
	where
		encodeEdge (a,b) = (encodeIndex nCols a, encodeIndex nCols b)
		rows = [ [((i,j),(i,j+1)) | j <- [0..nCols-2]]
		       | i <- [0..nRows-1]]
		columns = [ [((i,j),(i+1,j)) | j <- [0..nCols-1]]
			  | i <- [0..nRows-2]]
		diags = [ [((i,j),(i+1,j+1)) | j <- [0..nCols-2]]
			| i <- [0..nRows-2]]

