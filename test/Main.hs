{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module Main where


-------------------------------------------------------------------------------
import qualified Data.ByteString.Char8               as B
import           Data.Serialize                      as S
import           Database.Redis
import           Database.Redis.Utils
import           GHC.Generics
import           Test.Framework
import           Test.Framework.Providers.SmallCheck
import           Test.SmallCheck
import           Test.SmallCheck.Series
-------------------------------------------------------------------------------





main = do
    c <- connect defaultConnectInfo
    defaultMain [
         testProperty "simple BS roundtrip" (prop_roundtrip c)
       , testProperty "push/pop FIFO" (prop_fifo c)
       , testProperty "serialize custom type works" prop_serialize_works
       , testProperty "push/pop FIFO custom type" (prop_fifo_custom c)
       ]



prop_roundtrip :: Connection -> B.ByteString -> Property IO
prop_roundtrip c x = monadic $ runRedis c $ do
    lpush "testing" [x]
    res <- expect $ lpop "testing"
    return $ res == Just x


prop_fifo :: Connection -> Int -> String -> Property IO
prop_fifo c x y = monadic $ runRedis c $ do
    pushFIFO "testing" (x,y)
    [(a,b)] <- popFIFO "testing" 1
    return $ a == x && b == y



data TestData = TestData {
      tdInt :: Int
    , tdBS  :: B.ByteString
    } deriving (Eq, Generic, Show, Read)


instance Serialize TestData
instance Monad m => Serial m TestData


instance Monad m => Serial m B.ByteString where
    series = B.pack `fmap` series


prop_serialize_works :: TestData -> Bool
prop_serialize_works x = S.decode (S.encode x) == Right x

prop_fifo_custom :: Connection -> TestData -> Property IO
prop_fifo_custom c x = monadic $ runRedis c $ do
    pushFIFO "testing_custom" x
    [n] <- popFIFO "testing_custom" 1
    return $ n == x

