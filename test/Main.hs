{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module Main where


-------------------------------------------------------------------------------
import           Control.Retry
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
       , testProperty "locking works" (prop_locking c)
       , testProperty "short blockLock fails" (prop_blocklock_fail c)
       , testProperty "blockLock eventually succeeds" (prop_blocklock c)
       ]


-------------------------------------------------------------------------------
prop_locking :: Connection -> (B.ByteString, B.ByteString) -> Property IO
prop_locking c (ns, nm) = monadic $ do
  l <- runRedis c $ acquireLock ns 3 nm
  l' <- runRedis c $ acquireLock ns 3 nm
  runRedis c $ releaseLock ns nm
  return $ l && not l'


-------------------------------------------------------------------------------
prop_blocklock :: Connection -> (B.ByteString, B.ByteString) -> Property IO
prop_blocklock c (ns, nm) = monadic $ do
      l <- runRedis c $ blockLock redisPolicy ns 0.5 nm
      l' <- runRedis c $ blockLock redisPolicy ns 0.5 nm
      runRedis c $ releaseLock ns nm
      return $ l && l'
  where
    redisPolicy = capDelay 5000000 $ exponentialBackoff 25000 <> limitRetries 12


-------------------------------------------------------------------------------
prop_blocklock_fail c (ns, nm) = monadic $ do
    l <- runRedis c $ blockLock redisPolicy ns 3 nm
    l' <- runRedis c $ blockLock redisPolicy ns 3 nm
    runRedis c $ releaseLock ns nm
    return $ l && not l'
  where
    redisPolicy = constantDelay 500 <> limitRetries 2



-------------------------------------------------------------------------------
prop_roundtrip :: Connection -> B.ByteString -> Property IO
prop_roundtrip c x = monadic $ runRedis c $ do
    lpush "testing" [x]
    res <- unwrap $ lpop "testing"
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

