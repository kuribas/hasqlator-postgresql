{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeOperators #-}

module Database.PostgreSQL.Hasqlator.Typed
  ( -- * Database Types
    Table(..), Field(..), TableAlias, Nullable (..),
    
    
    -- * Querying
    Query, 

    -- * Selectors
    H.Selector, col, colMaybe,
    -- * Expressions
    Expression, Operator, 
    arg, argMaybe, (@@), nullable, cast, unsafeCast,
    op, fun1, fun2, fun3, (>.), (<.), (>=.), (<=.), (&&.), (||.), 

    -- * Insertion
    Insertor, insertValues, into, lensInto,
    insertOne,
    
    -- * imported from Database.PostgreSQL.Hasqlator
    H.Getter, H.ToSql, H.FromSql
  )
where
import Data.Text (Text)
import Data.Coerce
import qualified Data.ByteString as StrictBS
import Data.Scientific
import Data.Word
import Data.Int
import Data.Time
import Data.String
import qualified Data.Map.Strict as Map
import Control.Monad.State
import GHC.Exts (Constraint)
import GHC.TypeLits as TL
import Data.Functor.Contravariant
import GHC.Generics

import qualified Database.PostgreSQL.Hasqlator as H

data Nullable = Nullable | NotNull
data JoinType = LeftJoined | InnerJoined

type family CheckInsertable (fieldNullable :: Nullable) fieldType a
            :: Constraint where
  CheckInsertable 'Nullable a (Maybe a) = ()
  CheckInsertable 'Nullable a a = ()
  CheckInsertable 'NotNull a a = ()
  CheckInsertable t n ft =
    TypeError ('TL.Text "Cannot insert value of type " ':<>:
               'ShowType t ':<>:
               'TL.Text " into " ':<>:
               'ShowType n ':<>:
               'TL.Text " field of type " ':<>:
               'ShowType ft)

type Insertable nullable field a =
  (CheckInsertable nullable field a, H.ToSql a)

-- | check if field can be used in nullable context
type family CheckFieldNullable (leftJoined :: JoinType) (field :: Nullable)
  (context :: Nullable) :: Constraint
  where
    CheckFieldNullable 'InnerJoined 'Nullable 'Nullable = ()
    CheckFieldNullable _ 'Nullable 'NotNull =
      TypeError ('Text "A nullable field can be only used in a nullable context")
    -- a NotNull field can be used in both contexts
    CheckFieldNullable 'InnerJoined 'NotNull _ = ()
    -- any field can be used in a nullable context
    CheckFieldNullable 'LeftJoined _ 'Nullable = ()
    CheckFieldNullable 'LeftJoined _ 'NotNull =
      TypeError ('Text "A field from a left joined table can be only used in a nullable context.")

data Field table database (nullable :: Nullable) a =
  Field Text Text
 
newtype Expression (nullable :: Nullable) a =
  Expression { exprBuilder :: H.QueryBuilder }

instance IsString (Expression nullable Text) where
  fromString = arg . fromString

instance Semigroup (Expression nullable Text) where
  (<>) = fun2 (H.++.)

instance Monoid (Expression nullable Text) where
  mempty = arg ""

instance (Num n, H.ToSql n) => Num (Expression nullable n) where
  (+) = op (H.+.)
  (-) = op (H.-.)
  (*) = op (H.*.)
  negate = fun1 H.negate_
  abs = fun1 H.abs_
  signum = fun1 H.signum_
  fromInteger = arg . fromInteger

instance (Fractional n, H.ToSql n)
         => Fractional (Expression nullable n) where
  (/) = op (H./.)
  fromRational = arg . fromRational
  
data Table table database = Table Text
data TableAlias table database (joinType :: JoinType) =
  TableAlias Text

newtype Insertor table database a = Insertor (H.Insertor a)
  deriving (Monoid, Semigroup, Contravariant)

data ClauseState database = ClauseState
  { clauses :: H.QueryClauses  -- clauses build so far
  , aliases :: Map.Map Text Int   -- map of table names to times used
  }

emptyClauseState :: ClauseState database
emptyClauseState = ClauseState
  { clauses = mempty
  , aliases = Map.empty
  }

newtype Query database a = Query (State (ClauseState database) a)
  deriving (Functor, Applicative, Monad)

instance H.ToQueryBuilder (Query database (H.Selector a)) where
  toQueryBuilder (Query query) =
    let (selector, clauseState) = runState query emptyClauseState
    -- TODO: finalize query
    in H.toQueryBuilder $ H.select selector (clauses clauseState)

type Operator a b c = forall nullable .
                      (Expression nullable a ->
                       Expression nullable b ->
                       Expression nullable c)

-- | reference a field from a joined table
(@@) :: CheckFieldNullable leftJoined fieldNull exprNull
     => TableAlias table database leftJoined
     -> Field table database fieldNull a
     -> Expression exprNull a
TableAlias tableName @@ Field _ fieldName =
    Expression $ H.rawSql $ tableName <> "." <> fieldName

-- | make a selector from a column
col :: H.FromSql a
    => Expression 'NotNull a
    -> H.Selector a
col = coerce $ H.col . exprBuilder

-- | make a selector from a column that can be null
colMaybe :: H.FromSql (Maybe a)
         => Expression 'Nullable a
         -> H.Selector (Maybe a)
colMaybe = coerce $ H.col . exprBuilder

-- | pass an argument
arg :: H.ToSql a => a -> Expression nullable a
arg = Expression . H.arg

-- | pass an argument which can be null
argMaybe :: H.ToSql a => Maybe a -> Expression 'Nullable a
argMaybe = Expression . H.arg

-- | create an operator
op :: (H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder)
   -> Operator a b c
op = coerce

fun1 :: (H.QueryBuilder -> H.QueryBuilder)
     -> Expression nullable a
     -> Expression nullable b
fun1 = coerce

fun2 :: (H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder)
     -> Expression nullable a
     -> Expression nullable b
     -> Expression nullable c
fun2 = coerce

fun3 :: (H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder -> H.QueryBuilder)
     -> Expression nullable a
     -> Expression nullable b
     -> Expression nullable c
     -> Expression nullable d
fun3 = coerce

(>.), (<.), (>=.), (<=.) :: H.ToSql a => Operator a a Bool
(>.) = op (H.>.)
(<.) = op (H.<.)
(>=.) = op (H.>=.)
(<=.) = op (H.<=.)

(||.), (&&.) :: Operator Bool Bool Bool
(||.) = op (H.||.)
(&&.) = op (H.&&.)

-- | make expression nullable
nullable :: Expression nullable a -> Expression 'Nullable a
nullable = coerce

class Castable a where
  -- | Safe cast.  This uses the SQL CAST function to convert safely
  -- from one type to another.
  cast :: Expression nullable b
       -> Expression nullable a

castTo :: H.QueryBuilder
       -> Expression nullable b
       -> Expression nullable a
castTo tp e = Expression $ H.fun "cast" [exprBuilder e `H.as` tp]

instance Castable Float where
  cast = castTo "real"

instance Castable Double where
  cast = castTo "double precision"

instance Castable Int where
  cast = castTo "bigint"

instance Castable Int16 where
  cast = castTo "smallint"

instance Castable Int32 where
  cast = castTo "integer"

instance Castable Int64 where
  cast = castTo "bigint"


-- | Cast the return type of an expression to any other type, without
-- changing the query. Since this library adds static typing on top of
-- SQL, you may sometimes want to use this to get back the lenient
-- behaviour of SQL.  Obviously this opens up more possibilies for
-- runtime errors, so it's up to the programmer to ensure it's used
-- correctly.
unsafeCast :: Expression nullable a -> Expression nullable b
unsafeCast = coerce

fieldText :: Field table database nullable a -> Text
fieldText (Field table fieldName) = table <> "." <> fieldName

insertOne :: Insertable fieldNull fieldType a
          => Field table database fieldNull fieldType
          -> Insertor table database a
insertOne = Insertor . H.insertOne. fieldText

genFst :: (a :*: b) () -> a ()
genFst (a :*: _) = a

genSnd :: (a :*: b) () -> b ()
genSnd (_ :*: b) = b

class InsertGeneric table database (fields :: *) (data_ :: *) where
  insertDataGeneric :: fields -> Insertor table database data_

instance (InsertGeneric tbl db (a ()) (c ()),
          InsertGeneric tbl db (b ()) (d ())) =>
  InsertGeneric tbl db ((a :*: b) ()) ((c :*: d) ()) where
  insertDataGeneric (a :*: b) =
    contramap genFst (insertDataGeneric a) <>
    contramap genSnd (insertDataGeneric b)

instance InsertGeneric tbl db (a ()) (b ()) =>
  InsertGeneric tbl db (M1 m1 m2 a ()) (M1 m3 m4 b ()) where
  insertDataGeneric = contramap unM1 . insertDataGeneric . unM1

instance Insertable fieldNull a b =>
  InsertGeneric tbl db (K1 r (Field tbl db fieldNull a) ()) (K1 r b ()) where
  insertDataGeneric = contramap unK1 . insertOne . unK1

instance InsertGeneric tbl db (K1 r (Insertor tbl db a) ()) (K1 r a ()) where
  insertDataGeneric = contramap unK1 . unK1

insertData :: (Generic a, Generic b, InsertGeneric tbl db (Rep a ()) (Rep b ()))
           => a -> Insertor tbl db b
insertData = contramap from' . insertDataGeneric . from'
  where from' :: Generic a => a -> Rep a ()
        from' = from

skipInsert :: Insertor tbl db a
skipInsert = mempty

into :: Insertable fieldNull fieldType b
     => (a -> b)
     -> Field table database fieldNull fieldType
     -> Insertor table database a
into f = Insertor . H.into f . fieldText

lensInto :: Insertable fieldNull fieldType b
         => H.Getter a b
         -> Field table database fieldNull fieldType
         -> Insertor table database a
lensInto lens a = Insertor $ H.lensInto lens $ fieldText a

insertValues :: Table table database
             -> Insertor table database a
             -> [a]
             -> H.Command
insertValues (Table tableName) (Insertor i) =
  H.insertValues (H.rawSql tableName) i
