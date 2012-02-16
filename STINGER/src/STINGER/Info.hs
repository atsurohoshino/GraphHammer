-- |STINGER.Info
--
-- Defines a classes to store and read info for STINGER edges' and vertices' information.
--
-- Also defines instances of those classes for unit (). It is meant as a safe way
-- to say "no useful information". Its use as an info should not incur any cost.

{-# LANGUAGE GADTs, TypeFamilies, MultiParamTypeClasses, TypeSynonymInstances #-}

module STINGER.Info(
	  Info(..)
	, InfoArray(..)
	) where

import Data.Array

import G500.Index

-------------------------------------------------------------------------------
-- Classes for information storage.

-- |A class to store information.
class Info a where
	-- |A type we encode our values to.
	type EncodingType a

	-- |A method to encode value.
	encodeInfo :: a -> EncodingType a

	-- |Decoding process.
	decodeInfo :: EncodingType a -> a

-- |A class that defines "array interface" for STINGER information.
class (Num i, Ix i, Info a) => InfoArray i a where
	-- |A type for arrays with specified index.
	type EncodedInfoArray i a

	-- |Create an array.
	-- First parameter is a length of array.
	newEncodedInfoArray :: Monad m => a -> i -> m (EncodedInfoArray i a)

	-- |Get a value from array.
	readEncodedInfo :: Monad m => EncodedInfoArray i a -> i -> m a

	-- |Store an encoded value.
	storeEncodedInfo :: Monad m => EncodedInfoArray i a -> i -> a -> m ()

-------------------------------------------------------------------------------
-- Instances for () type.

instance Info () where
	type EncodingType () = ()
	encodeInfo = const ()
	decodeInfo = const ()

instance InfoArray Index () where
	type EncodedInfoArray Index () = ()
	newEncodedInfoArray _ _ = return ()
	readEncodedInfo _ _ = return ()
	storeEncodedInfo _ _ _ = return ()

instance InfoArray Int () where
	type EncodedInfoArray Int () = ()
	newEncodedInfoArray _ _ = return ()
	readEncodedInfo _ _ = return ()
	storeEncodedInfo _ _ _ = return ()
