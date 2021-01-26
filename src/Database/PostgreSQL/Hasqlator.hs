{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE PatternSynonyms #-}

{-|
Module      : Database.Hasqelator
Description : SQL generation
Copyright   : (c) Kristof Bastiaensen, 2020
License     : BSD-3
Maintainer  : kristof@resonata.be
Stability   : unstable
Portability : ghc


-}

module Database.PostgreSQL.Hasqlator
  ( -- * Querying
    Query, Command, select, mergeSelect, replaceSelect,

    -- * Query Clauses
    QueryClauses, from, innerJoin, leftJoin, rightJoin, outerJoin, emptyJoins,
    where_, emptyWhere, groupBy_, having, emptyHaving, orderBy, limit,
    limitOffset,

    -- * Selectors
    Selector, as,

    -- ** polymorphic selector
    col,
    -- ** specialised selectors
    -- | The following are specialised versions of `col`.  Using these
    -- may make refactoring easier, for example accidently swapping
    -- @`col` "age"@ and @`col` "name"@ would not give a type error,
    -- while @intCol "age"@ and @textCol "name"@ most likely would.
    intCol, doubleCol, floatCol, 

    -- ** other selectors
    values_,

    -- * Expressions
    subQuery,
    arg, fun, op, (>.), (<.), (>=.), (<=.), (+.), (-.), (*.), (/.), (=.), (++.),
    (/=.), (&&.), (||.), abs_, negate_, signum_, sum_, rawSql,

    -- * Insertion
    Insertor, insertValues, insertSelect, into, Getter, lensInto,
    insertOne, ToSql,
    
    -- * Rendering Queries
    renderQuery, SQLError(..), QueryBuilder, ToQueryBuilder(..),
    FromSql
  )

where

import Prelude hiding (unwords)
import Control.Monad.State
import Control.Applicative
import Data.Bifunctor
import Control.Monad.Except
import Control.Monad.Reader
import Data.Traversable
import Data.Monoid hiding ((<>))
import Data.String hiding (unwords)
import Data.List hiding (unwords)
import qualified Data.DList as DList
import Data.Proxy

import Database.PostgreSQL.LibPQ
import qualified PostgreSQL.Binary.Decoding as Decoding
import qualified PostgreSQL.Binary.Encoding as Encoding
import Data.DList (DList)
import Data.Word
import Data.Int
import qualified Data.ByteString as StrictBS
import ByteString.StrictBuilder (Builder)
import qualified ByteString.StrictBuilder as Builder
import qualified Data.Text.Encoding as Text
import Data.Text (Text)
import Data.Functor.Contravariant

class FromSql a where
  fromSql :: Maybe (Oid, StrictBS.ByteString) -> Either SQLError a

class ToSql a where
  toSqlByteString :: a -> Maybe StrictBS.ByteString
  toOid :: Proxy a -> Oid

toSqlValue :: forall a.ToSql a => a -> Maybe (Oid, StrictBS.ByteString)
toSqlValue a = (toOid (Proxy :: Proxy a),) <$> toSqlByteString a

instance FromSql a => IsString (Selector a) where
  fromString = col . fromString

class ToQueryBuilder a where
  toQueryBuilder :: a -> QueryBuilder

renderQuery :: ToQueryBuilder a
            => a -> (StrictBS.ByteString,
                     [Maybe (Oid, StrictBS.ByteString)])
renderQuery a = (Builder.builderBytes $ evalState stmt 1, DList.toList args)
  where QueryBuilder stmt args = toQueryBuilder a

selectOne :: (Maybe (Oid, StrictBS.ByteString) -> Either SQLError a)
          -> QueryBuilder -> Selector a
selectOne f fieldName =
  Selector (DList.singleton fieldName) $
  do resultSet <- ask
     SelectorState row column <- get
     maxCols <- liftIO $ nfields resultSet
     if column >= maxCols
       then throwError ResultSetCountError
       else do put (SelectorState row (column + 1))
               mbVal <- liftIO $ getvalue resultSet row column
               oidVal <- for mbVal $ \val -> 
                 (,val) <$> liftIO (ftype resultSet column)
               liftEither $ f oidVal

-- | The polymorphic selector.  The return type is determined by type
-- inference.
col :: FromSql a => QueryBuilder -> Selector a
col = selectOne fromSql

-- | an integer field (TINYINT.. BIGINT).  Any bounded haskell integer
-- type can be used here , for example `Int`, `Int32`, `Word32`.  An
-- `Overflow` ur `Underflow` error will be raised if the value doesn't
-- fit the type.
intCol :: (Show a, Bounded a, Integral a) => QueryBuilder -> Selector a
intCol = selectOne intFromSql

-- | a DOUBLE field.
doubleCol :: QueryBuilder -> Selector Double
doubleCol = col

-- | a FLOAT field.
floatCol :: QueryBuilder -> Selector Float
floatCol = col

data SQLError = SQLError Text
              | ResultSetCountError
              | TypeError StrictBS.ByteString String
              | Underflow
              | Overflow

data SelectorState = SelectorState Row Column

-- | Selectors contain the target fields or expressions in a SQL
-- SELECT statement, and perform the conversion to haskell.  Selectors
-- are instances of `Applicative`, so they can return the desired
-- haskell type.
data Selector a =
      Selector (DList QueryBuilder)
      (ReaderT Result (
          StateT SelectorState (
              ExceptT SQLError IO)) a)
                  
instance Functor Selector where
  fmap f (Selector cols cast) = Selector cols $ fmap f cast

instance Applicative Selector where
  Selector cols1 cast1 <*> Selector cols2 cast2 =
    Selector (cols1 <> cols2) (cast1 <*> cast2)
  pure x = Selector DList.empty $ pure x

instance Semigroup a => Semigroup (Selector a) where
  (<>) = liftA2 (<>)
    
instance Monoid a => Monoid (Selector a) where
  mempty = pure mempty

data Query a = Query (Selector a) QueryBody
data Command = Update QueryBuilder [(QueryBuilder, QueryBuilder)] QueryBody
             | InsertSelect QueryBuilder [QueryBuilder] [QueryBuilder] QueryBody
             | forall a.InsertValues QueryBuilder (Insertor a) [a]
             | forall a.Delete (Query a)

-- | An @`Insertor` a@ provides a mapping of parts of values of type
-- @a@ to columns in the database.  Insertors can be combined using `<>`.
data Insertor a =
  Insertor [Text] (a -> DList QueryBuilder)

data Join = Join JoinType [QueryBuilder] [QueryBuilder]
data JoinType = InnerJoin | LeftJoin | RightJoin | OuterJoin

instance ToQueryBuilder Command where
  toQueryBuilder (Update table setting body) =
    let pairQuery (a, b) = a <> " = " <> b
    in unwords [ "UPDATE", table
               , "SET", commaSep $ map pairQuery setting
               , toQueryBuilder body
               ] 
    
  toQueryBuilder (InsertValues table (Insertor cols convert) values__) =
    unwords [ "INSERT INTO"
            , table
            , parentized $ commaSep $ map rawSql cols
            , "VALUES"
            , commaSep $ map (parentized . commaSep . DList.toList . convert)
              values__
            ]
  toQueryBuilder (InsertSelect table cols rows queryBody) =
    unwords [ "INSERT INTO", table
            , parentized $ commaSep cols
            , "SELECT", parentized $ commaSep rows
            , toQueryBuilder queryBody
            ]
  toQueryBuilder (Delete query__) =
    "DELETE " <> toQueryBuilder query__

instance ToQueryBuilder QueryBody where
  toQueryBuilder body =
    unwords $ 
    fromB (_from body) <>
    (joinB <$> _joins body) <>
    renderPredicates "WHERE" (_where_ body) <>
    (groupByB $ _groupBy body) <>
    renderPredicates "HAVING" (_having body) <>
    orderByB (_orderBy body) <>
    limitB (_limit body)
    where
      fromB Nothing = []
      fromB (Just table) = ["FROM", table]

      joinB (Join _ [] _) = error "list of join tables cannot be empty"
      joinB (Join joinType tables joinConditions) =
        unwords $ [toQueryBuilder joinType, renderList tables] ++
        renderPredicates "ON" joinConditions

      groupByB [] = []
      groupByB e = ["GROUP BY", commaSep e]

      orderByB [] = []
      orderByB e = ["ORDER BY", commaSep $ map toQueryBuilder e]

      limitB Nothing = []
      limitB (Just (count, Nothing)) = ["LIMIT", fromString (show count)]
      limitB (Just (count, Just offset)) =
        [ "LIMIT" , fromString (show count)
        , "OFFSET", fromString (show offset) ]

instance ToQueryBuilder (Query a) where
  toQueryBuilder (Query _ body) = toQueryBuilder body

rawSql :: Text -> QueryBuilder
rawSql t = QueryBuilder builder DList.empty where
  builder = pure $ Builder.bytes $ Text.encodeUtf8 t
                                  
instance ToQueryBuilder JoinType where
  toQueryBuilder InnerJoin = "INNER JOIN"
  toQueryBuilder LeftJoin = "LEFT JOIN"
  toQueryBuilder RightJoin = "RIGHT JOIN"
  toQueryBuilder OuterJoin = "OUTER JOIN"

data QueryBody = QueryBody
  { _from :: Maybe QueryBuilder
  , _joins :: [Join]
  , _where_ :: [QueryBuilder]
  , _groupBy :: [QueryBuilder]
  , _having :: [QueryBuilder]
  , _orderBy :: [QueryOrdering]
  , _limit :: Maybe (Int, Maybe Int)
  }

data QueryOrdering = 
  Asc QueryBuilder | Desc QueryBuilder

instance ToQueryBuilder QueryOrdering where
  toQueryBuilder (Asc b) = b <> " ASC"
  toQueryBuilder (Desc b) = b <> " DESC"

data QueryBuilder =
  QueryBuilder (State Int Builder) (DList (Maybe (Oid, StrictBS.ByteString)))

instance IsString QueryBuilder where
  fromString s = QueryBuilder (pure $ foldMap Builder.utf8Char s) DList.empty

instance Semigroup QueryBuilder where
  QueryBuilder stmt1 vals1 <> QueryBuilder stmt2 vals2 =
    QueryBuilder (liftA2 (<>) stmt1 stmt2) (vals1 <> vals2)

instance Monoid QueryBuilder where
  mempty = QueryBuilder (pure mempty) mempty

newtype QueryClauses = QueryClauses (Endo QueryBody)
  deriving (Semigroup, Monoid)

instance Semigroup (Insertor a) where
  Insertor fields1 conv1 <> Insertor fields2 conv2 =
    Insertor (fields1 <> fields2) (conv1 <> conv2)

instance Monoid (Insertor a) where
  mempty = Insertor mempty mempty

instance Contravariant Insertor where
  contramap f (Insertor x g) = Insertor x (g . f)

class HasQueryClauses a where
  mergeClauses :: a -> QueryClauses -> a

instance HasQueryClauses (Query a) where
  mergeClauses (Query selector body) (QueryClauses clauses) =
    Query selector (clauses `appEndo` body)

instance HasQueryClauses Command where
  mergeClauses (Update table setting body) (QueryClauses clauses) =
    Update table setting (clauses `appEndo` body)
  mergeClauses (InsertSelect table toColumns fromColumns queryBody)
    (QueryClauses clauses) =
    InsertSelect table toColumns fromColumns (appEndo clauses queryBody)
  mergeClauses command__@InsertValues{} _ =
    command__
  mergeClauses (Delete query__) clauses =
    Delete $ mergeClauses query__ clauses
  
fromText :: Text -> QueryBuilder
fromText s = QueryBuilder (pure $ Builder.bytes $ Text.encodeUtf8 s) DList.empty

sepBy :: Monoid a => a -> [a] -> a
sepBy sep builder = mconcat $ intersperse sep builder
{-# INLINE sepBy #-}

commaSep :: (IsString a, Monoid a) => [a] -> a
commaSep = sepBy ", "
{-# INLINE commaSep #-}

unwords :: (IsString a, Monoid a) => [a] -> a
unwords = sepBy " "
{-# INLINE unwords #-}

parentized :: (IsString a, Monoid a) => a -> a
parentized expr = "(" <> expr <> ")"
{-# INLINE parentized #-}

renderList :: [QueryBuilder] -> QueryBuilder
renderList [] = ""
renderList [e] = e
renderList es = parentized $ commaSep es

renderPredicates :: QueryBuilder -> [QueryBuilder] -> [QueryBuilder]
renderPredicates _ [] = []
renderPredicates keyword [e] = [keyword, e]
renderPredicates keyword es =
  keyword : intersperse "AND" (map parentized $ reverse es)

arg :: forall a.ToSql a => a -> QueryBuilder
arg a = QueryBuilder 
  (state $ \i -> (foldMap Builder.utf8Char $ '$': show i, i+1))
  (DList.singleton $ toSqlValue a)

fun :: Text -> [QueryBuilder] -> QueryBuilder
fun name exprs = fromText name <> parentized (commaSep exprs)

op :: Text -> QueryBuilder -> QueryBuilder -> QueryBuilder
op name e1 e2 = parentized $ e1 <> " " <> fromText name <> " " <> e2

(>.), (<.), (>=.), (<=.), (+.), (-.), (/.), (*.), (=.), (/=.), (++.), (&&.),
  (||.)
  :: QueryBuilder -> QueryBuilder -> QueryBuilder
(>.) = op ">"
(<.) = op "<"
(>=.) = op ">="
(<=.) = op "<="
(+.) = op "+"
(*.) = op "*"
(/.) = op "/"
(-.) = op "-"
(=.) = op "="
(/=.) = op "<>"
a ++. b = fun "concat" [a, b]
(&&.) = op "and"
(||.) = op "or"

abs_, signum_, negate_, sum_ :: QueryBuilder -> QueryBuilder
abs_ x = fun "abs" [x]
signum_ x = fun "sign" [x]
negate_ x = fun "-" [x]
sum_ x = fun "sum" [x]


-- | insert a single value directly
insertOne :: forall a.ToSql a => Text -> Insertor a
insertOne s = Insertor [s] (DList.singleton . arg)

-- | insert a datastructure


-- | `into` uses the given extractor function to map the part to a
-- field.  For example:
--
-- > insertValues "Person" (fst `into` "name" <> snd `into` "age")
-- >   [("Bart Simpson", 10), ("Lisa Simpson", 8)]
into :: ToSql b => (a -> b) -> Text -> Insertor a
into toVal = contramap toVal . insertOne

-- | A Getter type compatible with the lens library
type Getter s a = (a -> Const a a) -> s -> Const a s

-- | `lensInto` uses a lens to map the part to a field.  For example:
--
-- > insertValues "Person" (_1 `lensInto` "name" <> _2 `lensInto` "age")
-- >   [("Bart Simpson", 10), ("Lisa Simpson", 8)]

lensInto :: ToSql b => Getter a b -> Text -> Insertor a
lensInto lens = into (getConst . lens Const)

subQuery :: ToQueryBuilder a => a -> QueryBuilder
subQuery = parentized . toQueryBuilder
  
from :: QueryBuilder -> QueryClauses
from table = QueryClauses $ Endo $ \qc -> qc {_from = Just table}

joinClause :: JoinType -> [QueryBuilder] -> [QueryBuilder] -> QueryClauses
joinClause tp tables conditions = QueryClauses $ Endo $ \qc ->
  qc { _joins = Join tp tables conditions : _joins qc }

innerJoin :: [QueryBuilder] -> [QueryBuilder] -> QueryClauses
innerJoin = joinClause InnerJoin

leftJoin :: [QueryBuilder] -> [QueryBuilder] -> QueryClauses
leftJoin = joinClause LeftJoin

rightJoin :: [QueryBuilder] -> [QueryBuilder] -> QueryClauses
rightJoin = joinClause RightJoin

outerJoin :: [QueryBuilder] -> [QueryBuilder] -> QueryClauses
outerJoin = joinClause OuterJoin

emptyJoins :: QueryClauses
emptyJoins = QueryClauses $ Endo $ \qc ->
  qc { _joins = [] }

where_ :: [QueryBuilder] -> QueryClauses
where_ conditions = QueryClauses $ Endo $ \qc ->
  qc { _where_ = reverse conditions ++ _where_ qc}

emptyWhere :: QueryClauses
emptyWhere = QueryClauses $ Endo $ \qc ->
  qc { _where_ = [] }

groupBy_ :: [QueryBuilder] -> QueryClauses
groupBy_ columns = QueryClauses $ Endo $ \qc ->
  qc { _groupBy = columns }

having :: [QueryBuilder] -> QueryClauses
having conditions = QueryClauses $ Endo $ \qc ->
  qc { _having = reverse conditions ++ _having qc }

emptyHaving :: QueryClauses
emptyHaving = QueryClauses $ Endo $ \qc ->
  qc { _having = [] }

orderBy :: [QueryOrdering] -> QueryClauses
orderBy ordering = QueryClauses $ Endo $ \qc ->
  qc { _orderBy = ordering }

limit :: Int -> QueryClauses
limit count = QueryClauses $ Endo $ \qc ->
  qc { _limit = Just (count, Nothing) }

limitOffset :: Int -> Int -> QueryClauses
limitOffset count offset = QueryClauses $ Endo $ \qc ->
  qc { _limit = Just (count, Just offset) }

emptyQueryBody :: QueryBody
emptyQueryBody = QueryBody Nothing [] [] [] [] [] Nothing 

select :: Selector a -> QueryClauses -> Query a
select selector (QueryClauses clauses) =
  Query selector $ clauses `appEndo` emptyQueryBody

mergeSelect :: Query b -> (a -> b -> c) -> Selector a -> Query c
mergeSelect (Query selector2 body) f selector1 =
  Query (liftA2 f selector1 selector2) body

replaceSelect :: Selector a -> Query b -> Query a
replaceSelect s (Query _ body) = Query s body

insertValues :: QueryBuilder -> Insertor a -> [a] -> Command
insertValues = InsertValues

insertSelect :: QueryBuilder -> [QueryBuilder] -> [QueryBuilder] -> QueryClauses
             -> Command
insertSelect table toColumns fromColumns (QueryClauses clauses) =
  InsertSelect table toColumns fromColumns $ appEndo clauses emptyQueryBody

-- | combinator for aliasing columns.
as :: QueryBuilder -> QueryBuilder -> QueryBuilder
as e1 e2 = e1 <> " AS " <> e2

-- | Ignore the content of the given columns
values_ :: [QueryBuilder] -> Selector ()
values_ cols = Selector (DList.fromList cols) $ do
  SelectorState row (Col col_) <- get
  put $ SelectorState row (Col $ col_ + fromIntegral (length cols))

decodeValue :: Decoding.Value a -> StrictBS.ByteString -> Either SQLError a
decodeValue v bs = first SQLError $ Decoding.valueParser v bs

-- Oids taken from pg_type.dat from the postgresql source.  Just google it.
pattern Int64Oid, Int32Oid, Int16Oid, FloatOid, DoubleOid, TextOid :: Oid
pattern Int64Oid = Oid 20
pattern Int16Oid = Oid 21
pattern Int32Oid = Oid 23
pattern TextOid = Oid 25
pattern FloatOid = Oid 700
pattern DoubleOid = Oid 701

oidType :: Oid -> StrictBS.ByteString
oidType Int64Oid = "int64"
oidType Int32Oid = "int32"
oidType Int16Oid = "int16"
oidType FloatOid = "float"
oidType DoubleOid = "double"
oidType _ = "unsupported type"

typeError :: Maybe (Oid, StrictBS.ByteString) -> String -> Either SQLError a
typeError Nothing str = Left $ TypeError "null"  str
typeError (Just (oid, _)) str = Left $ TypeError (oidType oid) str

-- selector for any bounded integer type
intFromSql :: forall a.(Show a, Bounded a, Integral a)
            => Maybe (Oid, StrictBS.ByteString) -> Either SQLError  a
intFromSql Nothing = Left $ TypeError "null" $
  "Int (" <> show (minBound :: a) <> ", " <> show (maxBound :: a) <> ")"
intFromSql (Just(oid, bs)) = case oid of
  Int64Oid -> decodeInt
  Int32Oid -> decodeInt
  Int16Oid -> decodeInt
  _ -> Left $ TypeError (oidType oid) $
       "Int (" <> show (minBound :: a) <> ", " <> show (maxBound :: a) <> ")"
  where decodeInt = castFromInt =<< decodeValue Decoding.int bs
        castFromInt :: Int64 -> Either SQLError a
        castFromInt i
          | i < fromIntegral (minBound :: a) = throwError Underflow
          | i > fromIntegral (maxBound :: a) = throwError Overflow
          | otherwise = pure $ fromIntegral i

instance FromSql a => FromSql (Maybe a) where
  fromSql Nothing = Right Nothing
  fromSql (Just x) = Just <$> fromSql (Just x)

instance FromSql Int where
  fromSql = intFromSql

instance FromSql Word where
  fromSql = intFromSql

instance FromSql Int16 where
  fromSql = intFromSql

instance FromSql Word16 where
  fromSql = intFromSql

instance FromSql Int32 where
  fromSql = intFromSql

instance FromSql Word32 where
  fromSql = intFromSql

instance FromSql Int64 where
  fromSql = intFromSql

instance FromSql Word64 where
  fromSql = intFromSql

instance FromSql Float where
  fromSql (Just (FloatOid, x)) = decodeValue Decoding.float4 x
  fromSql other = typeError other "Float"

instance FromSql Double where
  fromSql (Just (FloatOid, x)) = realToFrac <$> decodeValue Decoding.float4 x
  fromSql (Just (DoubleOid, x)) = decodeValue Decoding.float8 x
  fromSql other = typeError other "Double"

instance FromSql Text where
  fromSql (Just (TextOid, x)) = decodeValue Decoding.text_strict x
  fromSql other = typeError other "Text"

instance ToSql a => ToSql (Maybe a) where
  toSqlByteString Nothing = Nothing
  toSqlByteString (Just x) = toSqlByteString x
  toOid p = toOid p

instance ToSql Int where
  toSqlByteString = Just . Builder.builderBytes . Encoding.int8_int64 .
                    fromIntegral 
  toOid _ = Int64Oid

instance ToSql Int16 where
  toSqlByteString = Just . Builder.builderBytes . Encoding.int2_int16
  toOid _ = Int16Oid

instance ToSql Word16 where
  toSqlByteString = Just . Builder.builderBytes . Encoding.int2_word16
  toOid _ = Int16Oid

instance ToSql Int32 where
  toSqlByteString = Just . Builder.builderBytes . Encoding.int4_int32
  toOid _ = Int32Oid

instance ToSql Word32 where
  toSqlByteString = Just . Builder.builderBytes . Encoding.int4_word32
  toOid _ = Int32Oid

instance ToSql Int64 where
  toSqlByteString = Just . Builder.builderBytes . Encoding.int8_int64
  toOid _ = Int64Oid

instance ToSql Word64 where
  toSqlByteString = Just . Builder.builderBytes . Encoding.int8_word64
  toOid _ = Int64Oid

instance ToSql Float where
  toSqlByteString = Just . Builder.builderBytes . Encoding.float4
  toOid _ = FloatOid

instance ToSql Double where
  toSqlByteString = Just . Builder.builderBytes . Encoding.float8
  toOid _ = DoubleOid

instance ToSql Text where
  toSqlByteString = Just . Builder.builderBytes . Encoding.text_strict
  toOid _ = TextOid
  
