-- |STINGER.SimplestWithArrays
--
-- Simple API implementation with arrays.
--
-- THIS SUCKS!!!
--
-- The speed is down the toilet. I cannot wait long enough for a problem
-- solved in under a minute with a Simplest.hs.

{-# LANGUAGE GADTs, TypeFamilies, MultiParamTypeClasses, TypeOperators, FunctionalDependencies #-}
{-# LANGUAGE FlexibleContexts, FlexibleInstances, UndecidableInstances, EmptyDataDecls #-}
{-# LANGUAGE IncoherentInstances #-}

module STINGER.SimplestWithArrays(
	  Index	-- from G500.
	-- HList
	, Nil
	, (:.)

	-- representation exported abstractly
	, STINGER
	-- How to create a new STINGER.
	, stingerNew

	-- An analysis monad to create operations with STINGER.
	, STINGERM
	, runAnalysesStack

	-- local values processing.
	, Value
	, Composed
	, localValue
	, cst
	, ($=)
	, (+.), (-.), (*.), divV
	, (===), (=/=)

	-- Analysis type. Abstract.
	, Analysis
	, AnM
	, onEdges
	, anIf
	, getAnalysisResult
	, putAnalysisResult
	, incrementAnalysisResult
	, RequiredAnalyses
	-- how to create basic analysis, one which does not depend on the other.
	, basicAnalysis
	-- derived analysis, dependent on some other.
	, derivedAnalysis
	, EnabledAnalysis, EnabledAnalyses
	) where

import Control.Monad
import Control.Monad.State
import Data.Array
import Data.Array.IO
import Data.Int
import Data.IORef
import qualified Data.Map as Map
import qualified Data.Set as Set
import Data.Word

import G500.Index
import STINGER.HList

-------------------------------------------------------------------------------
-- A very simple representation.

-- |A representation parametrized by analyses required.
data STINGER as = STINGER {
	  stingerEdges		:: IOArray Index (Set.Set Index)
	-- Results of analyses.
	, stingerAnalyses	:: Map.Map Index (Map.Map Int Int64)
	-- Nodes affected in current batch.
	, stingerNodesAffected	:: Set.Set Index 
	}

-- |Monad to operate with STINGER.
type STINGERM as a = StateT (STINGER as) IO a


-------------------------------------------------------------------------------
-- Main STINGER API.

stingerNew :: Index -> IO (STINGER as)
stingerNew maxVertices = do
	edges <- newArray (0,maxVertices-1) Set.empty
	return $ STINGER edges Map.empty Set.empty

stingerEdgeExists :: Index -> Index -> STINGERM as Bool
stingerEdgeExists start end
	| start == end = return True
	| otherwise = do
	edges <- liftM stingerEdges get
	ends <- liftIO $ readArray edges start
	return $ Set.member end ends

stingerInsertEdge :: Index -> Index -> STINGERM as ()
stingerInsertEdge start end
	| start == end = return ()
	| otherwise = do
		edges <- liftM stingerEdges get
		oldEnds <- liftIO $ readArray edges start
		oldStarts <- liftIO $ readArray edges end
		let ends = Set.insert end oldEnds
		let starts = Set.insert end oldEnds
		ends `seq` liftIO $ writeArray edges start ends
		starts `seq` liftIO $ writeArray edges end starts
		modify $ \st -> st {
			  stingerNodesAffected = Set.insert start $ Set.insert end $
				stingerNodesAffected st
			}
	

stingerGetEdges :: Index -> STINGERM as [Index]
stingerGetEdges start = do
	edges <- liftM stingerEdges get
	liftM Set.toList $ liftIO $ readArray edges start

stingerGetAnalysis :: Int -> Index -> STINGERM as Int64
stingerGetAnalysis analysisIndex index = 
	liftM (Map.findWithDefault 0 analysisIndex . Map.findWithDefault Map.empty index . stingerAnalyses) get

stingerSetAnalysis :: Int -> Index -> Int64 -> STINGERM as ()
stingerSetAnalysis analysisIndex index value = do
	modify $ \st -> st {
		  stingerAnalyses = Map.insertWith Map.union index (Map.singleton analysisIndex value) $
			stingerAnalyses st
		, stingerNodesAffected = Set.insert index $ stingerNodesAffected st
		}

stingerIncrementAnalysis :: Int -> Index -> Int64 -> STINGERM as ()
stingerIncrementAnalysis analysisIndex index incr = do
	modify $ \st -> st {
		  stingerAnalyses = Map.insertWith (Map.unionWith (+)) index (Map.singleton analysisIndex incr) $
			stingerAnalyses st
		, stingerNodesAffected = Set.insert index $ stingerNodesAffected st
		}

stingerDumpState :: STINGERM as ()
stingerDumpState = do
	st <- get
	let affected = Set.toList $ stingerNodesAffected st
	liftIO $ forM_ (take 100 affected) $ \node -> do
		putStrLn $ "Node "++show node
		case Map.lookup node (stingerAnalyses st) of
			Nothing -> return ()
			Just analyses -> forM_ (Map.toList analyses) $ \(a,v) -> do
				putStrLn $ "    Analysis "++show a++", value "++show v

stingerClearAffected :: STINGERM as ()
stingerClearAffected = modify $ \st -> st { stingerNodesAffected = Set.empty }

-- |Run the analyses stack.
-- Its' parameters:
--  - function to obtain edges to insert.
--  - a stack of analyses to perform.
runAnalysesStack :: Index -> IO (Maybe (Array Int Index)) ->
	Analysis as as -> IO ()
runAnalysesStack maxVertices receiveChanges analysesStack = do
	s <- stingerNew maxVertices
	(flip runStateT) s runLoop
	return ()
	where
		pairs (x:y:xys) = (x,y) : pairs xys
		pairs _ = []
		runLoop = do
			edges <- liftIO receiveChanges
			case edges of
				Nothing -> return ()
				Just edges -> do
					let es = pairs $ Data.Array.elems edges
					stingerClearAffected
					performInsertionAndAnalyses es
					stingerDumpState
					runLoop
		performInsertionAndAnalyses edgeList = do
--			liftIO $ putStrLn "performInsertionAndAnalyses!!!"
			insertAndAnalyzeSimpleSequential analysesStack edgeList

insertAndAnalyzeSimpleSequential :: Analysis as' as -> [(Index, Index)] -> STINGERM as ()
insertAndAnalyzeSimpleSequential stack edges = do
	forM_ edges $ \(start, end) -> do
		exists <- stingerEdgeExists start end
		if exists then return ()
			else do
--				liftIO $ putStrLn $ "Before inserting "++show (start,end)
				runStack stack start end
				stingerInsertEdge start end

runStack :: Analysis as' as -> Index -> Index -> STINGERM as ()
runStack EmptyAnalysis _ _ = return ()
runStack (Analysis req analysis action) start end = do
	runStack req start end
	interpret (action analysis (ValueConst start) (ValueConst end))


-------------------------------------------------------------------------------
-- Analysis construction monad.

class Storable v where
	toInt64 :: v -> Int64
	fromInt64 :: Int64 -> v

instance Storable Bool where
	toInt64 = fromIntegral . fromEnum
	fromInt64 = toEnum . fromIntegral

instance Storable Int where
	toInt64 = fromIntegral
	fromInt64 = fromIntegral

instance Storable Int64 where
	toInt64 = id
	fromInt64 = id

data AnStatement as where
	-- destination and value
	ASAssign :: Storable a => Value Asgn a -> Value _a a -> AnStatement as
	-- start vertex for edges, end vertex for edges (will be assigned in run-time),
	-- statements to perform.
	ASOnEdges :: Value _a Index -> Value Asgn Index -> [AnStatement as] -> AnStatement as
	ASAtomicIncr :: Int -> Value _a Index -> Value _b Int64 -> AnStatement as
	ASIf :: Value _a Bool -> [AnStatement as] -> [AnStatement as] -> AnStatement as
	ASSetAnalysisResult :: Int -> Value _a Index -> Value _b Int64 -> AnStatement as
	ASFlagVertex :: Value _a Index -> AnStatement as
	ASOnFlaggedVertices :: Value Asgn Index -> [AnStatement as] -> AnStatement as

data AnSt as = AnSt {
	  asValueIndex		:: Int
	, asStatements		:: [AnStatement as]
	}

-- this is how we construct analyses.
type AnM as a = State (AnSt as) a

addStatement :: AnStatement as -> AnM as ()
addStatement stat = modify $ \as -> as { asStatements = asStatements as ++ [stat] }

cutStatements :: AnM as r -> AnM as ([AnStatement as], r)
cutStatements act = do
	stats <- liftM asStatements get
	modify $ \as -> as { asStatements = []}
	r <- act
	eStats <- liftM asStatements get
	modify $ \as -> as { asStatements = stats }
	return (eStats, r)

-------------------------------------------------------------------------------
-- Analysis API - enumerating edges.

onEdges :: Value Composed Index -> (Value Composed Index -> AnM as r) -> AnM as r
onEdges vertex act = do
	neighbor <- defineLocal
	(eStats, r) <- cutStatements $ act $ ValueComposed neighbor
	addStatement $ ASOnEdges vertex neighbor eStats
	return r

-------------------------------------------------------------------------------
-- Analysis API - flagging vertices and iterating over them.

flagVertex :: Value any Index -> AnM as ()
flagVertex vertex = do
	addStatement $ ASFlagVertex vertex

onFlaggedVertices :: (Value Composed Index -> AnM as r) -> AnM as r
onFlaggedVertices action = do
	vertex <- defineLocal
	(eStats, r) <- cutStatements $ action $ ValueComposed vertex
	addStatement $ ASOnFlaggedVertices vertex eStats
	return r

-------------------------------------------------------------------------------
-- Analysis API - conditional operator.

anIf :: Value Composed Bool -> AnM as r -> AnM as r -> AnM as r
anIf cond th el = do
	(thStats, r) <- cutStatements th
	(elStats, _) <- cutStatements el
	addStatement $ ASIf cond thStats elStats
	return r

-------------------------------------------------------------------------------
-- Analysis API - keeping analyses' results.

getEnabledAnalyses :: AnM as as
getEnabledAnalyses = return (error "value of getEnabledAnalyses should not be requested.")

getAnalysisIndex :: AnalysisIndex a as => a -> AnM as Int
getAnalysisIndex a = do
	analyses <- getEnabledAnalyses
	return $ analysisIndex a analyses

-- |Fetch analysis result.
getAnalysisResult :: (AnalysisIndex a as) => a -> Value _a Index -> AnM as (Value Composed Int64)
getAnalysisResult analysis vertex = do
	index <- getAnalysisIndex analysis
	return $ ValueComposed $ ValueAnalysisResult index vertex

-- |Store analysis result.
putAnalysisResult :: (AnalysisIndex a as) => a -> Value _a Index -> Value _b Int64 -> AnM as ()
putAnalysisResult analysis vertex value = do
	index <- getAnalysisIndex analysis
	addStatement $ ASSetAnalysisResult index vertex value

-- |Update atomically result with increment.
incrementAnalysisResult :: (AnalysisIndex a as) => a -> Value _a Index -> Value _b Int64 -> AnM as ()
incrementAnalysisResult analysis vertex incr = do
	index <- getAnalysisIndex analysis
	addStatement $ ASAtomicIncr index vertex incr

-------------------------------------------------------------------------------
-- STINGER API - values and expressions.

-- value is assignable.
data Asgn
-- value is composite, thus not assignable.
data Composed

-- |A (modifable) value.
data Value asgn v where
	-- argument's index.
	ValueArgument :: Int -> Value Composed v 
	-- some local variable.
	ValueLocal :: Storable v => Int -> Value Asgn v
	-- constant. we cannot live wothout them.
	ValueConst :: v -> Value Composed v
	-- binary operation.
	ValueBin :: BinOp l r v -> Value _a l -> Value _b r -> Value Composed v
	-- and unary operations.
	ValueUn :: UnOp a v -> Value _a a -> Value Composed v
	-- cast as composed.
	ValueComposed :: Value _a v -> Value Composed v
	-- address of the analysis result of the value.
	-- index of the analysis in stack, vertex index.
	ValueAnalysisResult :: Int -> Value _b Index -> Value Asgn Int64

data BinOp x y z where
	Plus :: Num x => BinOp x x x
	Minus :: Num x => BinOp x x x
	Mul :: Num x => BinOp x x x
	Div :: Integral x => BinOp x x x
	Equal :: Eq x => BinOp x x Bool

data UnOp a r where
	Not :: UnOp Bool Bool
	Negate :: Num v => UnOp v v

-- |Define a (mutable) value local to a computation.
defineLocal :: Storable v => AnM as (Value Asgn v)
defineLocal = do
	modify $ \as -> as { asValueIndex = asValueIndex as + 1 }
	liftM (ValueLocal . asValueIndex) get

-- |Define a local value and assign to it.
localValue :: Storable v => v -> AnM as (Value Asgn v)
localValue def = do
	v <- defineLocal
	v $= cst def
	return v

infixl 6 +., -.
(+.), (-.) :: Num v => Value _a v -> Value _b v -> Value Composed v
a +. b = ValueBin Plus a b
a -. b = ValueBin Minus a b
a *. b = ValueBin Mul a b
divV a b = ValueBin Div a b

(===), (=/=) :: Eq v => Value _a v -> Value _b v -> Value Composed Bool
a === b = ValueBin Equal a b
a =/= b = notV $ a === b

notV = ValueUn Not
negV = ValueUn Negate

cst :: v -> Value Composed v
cst = ValueConst

-- |Assigning a value.
infixr 1 $=
($=) :: Storable v => Value Asgn v -> Value _a v -> AnM as ()
dest $= expr = addStatement $ ASAssign dest expr

-------------------------------------------------------------------------------
-- Interpreting analysis in STINGER monad.

data IntSt = IntSt {
	  istLocals	:: Map.Map Int Int64
	}

type AIM as a = StateT IntSt (StateT (STINGER as) IO) a

interpret :: EnabledAnalyses aas as => AnM aas a -> STINGERM as ()
interpret action = evalStateT
	(interpretStatements $ asStatements $ execState action (AnSt 0 []))
	(IntSt Map.empty)

interpretStatements :: EnabledAnalyses aas as => [AnStatement aas] -> AIM as ()
interpretStatements stats = mapM_ interpretStatement stats

interpretStatement :: EnabledAnalyses aas as => AnStatement aas -> AIM as ()
interpretStatement stat = case stat of
	ASAssign dest what -> assign dest what
	ASOnEdges startVertex vertexToAssign stats -> do
		start <- interpretValue startVertex
		edges <- lift $ stingerGetEdges start
		forM_ edges $ \edge -> do
			assign vertexToAssign $ cst edge
			interpretStatements stats
	ASAtomicIncr aIndex vIndex incr -> do
		incr <- interpretValue incr
		vIndex <- interpretValue vIndex
		lift $ stingerIncrementAnalysis aIndex vIndex incr
	ASIf cond thenStats elseStats -> do
		c <- interpretValue cond
		interpretStatements $ if c then thenStats else elseStats
	where
		assign :: Value Asgn v -> Value _b v -> AIM as ()
		assign (ValueLocal index) what = do
			x <- interpretValue what
			modify $ \ist -> ist { istLocals = Map.insert index (toInt64 x) $ istLocals ist }

interpretValue :: Value _a v -> AIM as v
interpretValue value = case value of
	ValueLocal index -> do
		mbV <- liftM (Map.lookup index . istLocals) get
		case mbV of
			Just v -> return (fromInt64 v)
			Nothing -> error "local variable not found."
	ValueArgument index -> error "interpreting ValueArgument!!!"
	ValueConst v -> return v
	ValueBin Plus l r -> interpretBin (+) l r
	ValueBin Minus l r -> interpretBin (-) l r
	ValueBin Mul l r -> interpretBin (*) l r
	ValueBin Div l r -> interpretBin (div) l r
	ValueBin Equal l r -> interpretBin (==) l r
	ValueUn Not val -> liftM not $ interpretValue val
	ValueUn Negate val -> liftM negate $ interpretValue val
	ValueComposed v -> interpretValue v
	ValueAnalysisResult analysisIndex vertex -> do
		v <- interpretValue vertex
		lift $ stingerGetAnalysis analysisIndex v
	where
		interpretBin :: (a -> b -> r) -> Value _a a -> Value _b b -> AIM as r
		interpretBin f a b = liftM2 f (interpretValue a) (interpretValue b)


-------------------------------------------------------------------------------
-- STINGER analysis combination.

type family RequiredAnalyses a

data AnalysisNotEnabled a

class EnabledAnalysis a as

class EnabledBool b a as
instance TyCast TRUE b => EnabledBool b  a (a  :. as)
instance (EnabledAnalysis a as) => EnabledBool FALSE a (a' :. as)

instance (EnabledBool b a (a' :. as), TyEq b a a') => EnabledAnalysis a (a' :. as)

class EnabledAnalyses as eas

instance EnabledAnalyses Nil eas
instance (EnabledAnalyses as eas, EnabledAnalysis a eas) => EnabledAnalyses (a :. as) eas

data Analysis as wholeset where
	Analysis :: (EnabledAnalyses (RequiredAnalyses a) as, EnabledAnalyses as wholeset, EnabledAnalyses (a :. as) wholeset) =>
			Analysis as wholeset -> a ->
			(a -> Value Composed Index -> Value Composed Index -> AnM (a :. as) ()) -> Analysis (a :. as) wholeset
	EmptyAnalysis :: Analysis Nil wholeset

basicAnalysis :: ((RequiredAnalyses a) ~ Nil, EnabledAnalysis a wholeset) =>
	a -> (a -> Value Composed Index -> Value Composed Index -> AnM (a :. Nil) ()) -> Analysis (a :. Nil) wholeset
basicAnalysis analysis edgeInsert = Analysis EmptyAnalysis analysis edgeInsert

derivedAnalysis :: (EnabledAnalyses (RequiredAnalyses a) as, EnabledAnalyses as wholeset, EnabledAnalyses (a :. as) wholeset)  =>
	Analysis as wholeset -> a -> (a -> Value Composed Index -> Value Composed Index -> AnM (a :. as) ()) -> Analysis (a :. as) wholeset
derivedAnalysis required analysis edgeInsert = Analysis required analysis edgeInsert

class EnabledAnalysis a as => AnalysisIndex a as where
	analysisIndex :: a -> as -> Int

class AnalysisIndexBool b a as | a as -> b where
	analysisAtHead :: a -> as -> b
	analysisIndexBool :: b -> a -> as -> Int
instance (HLength as, TyCast TRUE b) => AnalysisIndexBool b  a (a  :. as) where
	analysisAtHead _ _ = undefined
	analysisIndexBool _ _ list = hLength $ hTail list
instance (AnalysisIndex a as) => AnalysisIndexBool FALSE a (a' :. as) where
	analysisAtHead _ _ = undefined
	analysisIndexBool _ a list = analysisIndex a $ hTail list

instance (EnabledBool b a (a' :. as), AnalysisIndexBool b a (a' :. as), TyEq b a a') => AnalysisIndex a (a' :. as) where
	analysisIndex a as = analysisIndexBool (analysisAtHead a as) a as

