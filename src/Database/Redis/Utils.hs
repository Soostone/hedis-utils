{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

module Database.Redis.Utils
    (

     -- * General Utilities
      unwrap
    , unexpected
    , retryRedis
    , runRedisSimple

     -- * Locking
    , blocking, defBlockPolicy
    , blockLock
    , acquireLock
    , releaseLock
    , mkLockName
    , mkTimeOut

     -- * Renewable Locks
    , blockRenewableLock
    , acquireRenewableLock
    , renewRenewableLock
    , releaseRenewableLock

    -- * FIFO Queue Operations
    , pushFIFO
    , popFIFO

    -- * Transformer utils
    , maybeRedisT
    , redisT

    ) where


-------------------------------------------------------------------------------
import           Control.Error
import           Control.Exception
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Retry
import           Data.ByteString.Char8  (ByteString)
import qualified Data.ByteString.Char8  as B
import           Data.Default
import           Data.Maybe
import           Data.Monoid
import           Data.Serialize         as S
import           Data.Time.Clock.POSIX
import           Database.Redis         hiding (decode)
import           Prelude
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- | Short-hand for running redis and retrying a few times.
runRedisSimple :: Connection -> Redis a -> IO a
runRedisSimple = retryRedis 3 "Generic retryRedis"


-------------------------------------------------------------------------------
-- | Retry a given redis action multiple times if it fails.
retryRedis
    :: Int
    -- ^ Number of retries
    -> String
    -- ^ Additional message to display
    -> Connection
    -- ^ Connection pool
    -> Redis a
    -- ^ Action to run
    -> IO a
retryRedis mx msg c f = go `catch` handleEx
  where
    handleEx :: SomeException -> IO a
    handleEx e = error . concat $
           [ "Hedis: Retried ", show mx, " times but failed. Error: "
           , show e, ". Message: ", msg ]
    go = recoverAll (def <> limitRetries mx) $ const $ runRedis c f




-------------------------------------------------------------------------------
-- | Unwrap just enough to know if there was an exception. Expect that
-- there isn't one, getting rid of Either return type from hedis.
unwrap :: (Monad m, Show a) => m (Either a b) -> m b
unwrap f = do
  res <- f
  case res of
    Left e -> unexpected e
    Right xs -> return xs


-------------------------------------------------------------------------------
-- | Raise the unexpected exception
unexpected :: Show a => a -> t
unexpected r = error $ "Received an unexpected Left response from Redis. Reply: " ++ show r



-------------------------------------------------------------------------------
-- | Lift the annoying Redis (Either Reply) return type into an 'ExceptT'.
redisT :: (Functor m, Show e) => m (Either e b) -> ExceptT String m b
redisT f = bimapExceptT show id (ExceptT f)


-------------------------------------------------------------------------------
-- | Lift the even more annyong Redis (Either Reply (Maybe a)) type
-- into an ExceptT.
maybeRedisT :: (Monad m, Functor m, Show e) => m (Either e (Maybe b)) -> ExceptT String m b
maybeRedisT f = do
    res <- redisT f
    case res of
      Nothing -> throwE "redis returned no object (Nothing)"
      Just x -> return x






                                 -------------
                                 -- Locking --
                                 -------------


-------------------------------------------------------------------------------
-- | A good default; 10ms initial, exponential backoff with max 10
-- retries and a cap of 1 second delay.
defBlockPolicy :: RetryPolicy
defBlockPolicy = capDelay 1000000 $
  mempty <> limitRetries 10 <> exponentialBackoff 10000


-------------------------------------------------------------------------------
-- | Block until given action returns True.
blocking :: MonadIO m => RetryPolicy -> m Bool -> m Bool
blocking settings f = retrying settings (const $ return . not) (const f)


-------------------------------------------------------------------------------
-- | Block until lock can be acquired.
--
-- This function implements a solid locking mechanism using the
-- algorithm described in one of the redis.io comments. It uses getset
-- underneath via 'acquireLock'.
blockLock
    :: RetryPolicy
    -- ^ Retry settings for while trying to acquire the lock. As an
    -- example, a 25 milisecond base with 10 exp backoff retries would
    -- work up to a 25 second retry.
    -> B.ByteString
    -- ^ namespace for this lock
    -> Double
    -- ^ timeout in seconds.
    -> B.ByteString
    -- ^ Name of item to lock.
    -> Redis Bool
blockLock settings lock to nm = blocking settings $ acquireLock lock to nm



-------------------------------------------------------------------------------
-- | Try to acquire lock in a given namespace. Immediately returns the
-- result, so you need to keep trying. Use 'blockLock' instead for a
-- higher level wrapper.
acquireLock
    :: B.ByteString
    -- ^ namespace for this lock
    -> Double
    -- ^ timeout in seconds
    -> B.ByteString
    -- ^ Name to lock
    -> Redis Bool
acquireLock lock to nm = do
    res <- setOpts nm' val opts
    case res of
      Right Ok -> return True
      _        -> return False
    where
      -- | We no longer use the lock value. We could set it to a
      -- unique id and return it as a handle to make more secure locks
      -- in the future.
      val = mempty
      opts = SetOpts
        { setSeconds = Nothing
        , setMilliseconds = Just (doubleSecondsToMillis to)
        , setCondition = Just Nx -- set if not set
        }
      nm' = mkLockName lock nm



-------------------------------------------------------------------------------
-- | Release a lock
releaseLock
    :: B.ByteString
    -- ^ namespace for this lock
    -> B.ByteString
    -- ^ Name of item to release
    -> Redis ()
releaseLock lock nm = unwrap (del [nm']) >> return ()
    where
      nm' = mkLockName lock nm


                            ---------------------
                            -- Renewable Locks --
                            ---------------------


-------------------------------------------------------------------------------
{-# DEPRECATED blockRenewableLock "Use blockLock" #-}
-- | Like blockLock, but for renewable locks.
blockRenewableLock
    :: RetryPolicy
    -- ^ Retry settings for while trying to acquire the lock. As an
    -- example, a 25 milisecond base with 10 exp backoff retries would
    -- work up to a 25 second retry.
    -> B.ByteString
    -- ^ namespace for this lock
    -> Double
    -- ^ timeout in seconds.
    -> B.ByteString
    -- ^ Name of item to lock.
    -> Redis Bool
blockRenewableLock = blockLock


-------------------------------------------------------------------------------
{-# DEPRECATED acquireRenewableLock "Use acquireLock" #-}

-- | Like acquireLock, but for renewable locks.
acquireRenewableLock
    :: B.ByteString
    -- ^ namespace for this lock
    -> Double
    -- ^ timeout in seconds
    -> B.ByteString
    -- ^ Name to lock
    -> Redis Bool
acquireRenewableLock = acquireLock


------------------------------------------------------------------------------
-- | Renews a renewable lock.
renewRenewableLock
    :: B.ByteString
    -- ^ namespace for this lock
    -> Double
    -- ^ timeout in seconds.
    -> B.ByteString
    -- ^ Name to lock
    -> Redis Bool
renewRenewableLock lock to nm = do
    res <- pexpire nm' (doubleSecondsToMillis to)
    return $ case res of
      Right True -> True
      _          -> False
  where
    nm' = mkLockName lock nm


-------------------------------------------------------------------------------
{-# DEPRECATED releaseRenewableLock "Use releaseLock" #-}
-- | Release a renewable lock
releaseRenewableLock
    :: B.ByteString
    -- ^ namespace for this lock
    -> B.ByteString
    -- ^ Name of item to release
    -> Redis Bool
releaseRenewableLock ns nm  = True <$ releaseLock ns nm


-------------------------------------------------------------------------------
mkLockName :: ByteString -> ByteString -> ByteString
mkLockName lock nm = B.intercalate ":" [pfx, lock, nm]
  where
    pfx = "_lock" <> protocolVersion


-------------------------------------------------------------------------------
-- | This very rarely needs to be bumped to invalidate old batches of
-- locks when undergoing version updates.
--
-- 2 (0.4.0) - Keys never inherently expired but would expire on
-- read. As such, unclean shutdowns when going between versions of
-- this library would see a key that would never expire on its own and
-- would never be able to lock that key again. Setting this protocol
-- version means on boot, different locks will be taken.
protocolVersion :: ByteString
protocolVersion = "2"


-------------------------------------------------------------------------------
mkTimeOut :: (MonadIO m) => Double -> m B.ByteString
mkTimeOut to = (B.pack . show . (to +)) `liftM` liftIO getTime



getTime :: IO Double
getTime = realToFrac `fmap` getPOSIXTime



                                 ------------
                                 -- Queues --
                                 ------------


------------------------------------------------------------------------------
-- | Push item into a FIFO buffer
pushFIFO :: (Serialize a) => ByteString -> a -> Redis ()
pushFIFO k x = do
  !_ <- unwrap $ lpush k [S.encode x]
  return ()


------------------------------------------------------------------------------
-- | Collect redis list into Haskell list, popping elements one at a time up to
-- n elements
--
-- This should be atomic, but other processes may take items out of the same
-- buffer in an interleaved fashion.
popFIFO :: (Serialize a) => ByteString -> Int -> Redis [a]
popFIFO k n = do
  res <- replicateM n $ rpop k
  case sequence res of
    Left r -> unexpected r
    Right xs -> return $ map conv . catMaybes $ xs
  where
    conv x = case S.decode x of
               Left e -> error $ "Serialize.decode conversion failed: " ++ e
               Right x' -> x'




-------------------------------------------------------------------------------
doubleSecondsToMillis :: (Integral a) => Double -> a
doubleSecondsToMillis = round . (*1000)
