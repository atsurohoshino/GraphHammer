-- |GraphHammer.HList
-- Copyright : (C) 2013 Parallel Scientific Labs, LLC.
-- License   : GPLv2
--
-- Home-brewn HList.

{-# LANGUAGE TypeFamilies, MultiParamTypeClasses, TypeOperators, FunctionalDependencies #-}
{-# LANGUAGE FlexibleContexts, FlexibleInstances, UndecidableInstances, EmptyDataDecls #-}
{-# LANGUAGE IncoherentInstances #-}


module GraphHammer.HList where

data Nil

infixr 5 :.
data a :. b

hHead :: a :. as -> a
hHead = undefined

hTail :: a :. as -> as
hTail = undefined

data FALSE
data TRUE

class FAIL a	-- no instances!

class TyCast a b | a -> b, b -> a
instance TyCast a a

class TyOr x y r | x y -> r
instance TyOr TRUE  TRUE  TRUE
instance TyOr TRUE  FALSE TRUE
instance TyOr FALSE TRUE  TRUE
instance TyOr FALSE FALSE FALSE

class TyEq b x y | x y -> b
instance TyEq FALSE x y
instance TyCast TRUE b => TyEq b  x x

-- type-level arithmetic.
data Z
data S n

class Nat a where
	natural :: a -> Int

instance Nat Z where
	natural = const 0

fromSucc :: S n -> n
fromSucc = undefined

instance Nat n => Nat (S n) where
	natural = (1+) . natural . fromSucc

class HLength a where
	hLength :: a -> Int
instance HLength Nil where hLength = const 0
instance HLength as => HLength (a :. as) where hLength list = 1 + hLength (hTail list)
