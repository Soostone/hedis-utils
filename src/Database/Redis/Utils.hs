{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}

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
import qualified Database.Redis         as R
import           Prelude                hiding (catch)
import           Safe
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
retryRedis max msg c f = go `catch` handle
  where
    handle :: SomeException -> IO a
    handle e = error . concat $
           [ "Hedis: Retried ", show max, " times but failed. Error: "
           , show e, ". Message: ", msg ]
    go = recoverAll (def <> limitRetries max) $ runRedis c f




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
-- | Lift the annoying Redis (Either Reply) return type into an 'EitherT'.
redisT :: (Functor m, Show e) => m (Either e b) -> EitherT String m b
redisT f = bimapEitherT show id (EitherT f)


-------------------------------------------------------------------------------
-- | Lift the even more annyong Redis (Either Reply (Maybe a)) type
-- into an EitherT.
maybeRedisT :: (Monad m, Functor m, Show e) => m (Either e (Maybe b)) -> EitherT String m b
maybeRedisT f = do
    res <- redisT f
    case res of
      Nothing -> left "redis returned no object (Nothing)"
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
blocking set f = retrying set (const $ return . not) f


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
blockLock set lock to nm = blocking set $ acquireLock lock to nm



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
    tm <- mkTimeOut to
    res <- unwrap $ setnx nm' tm
    case res of
      True -> return True
      False -> do
          curLock <- unwrap $ R.get nm'
          case curLock of
            -- someone else unlocked it, retry the process
            Nothing -> acquireLock lock to nm
            Just curVal ->
              case readMay $ B.unpack curVal of
                -- there is a value in there but I can't even read it
                Nothing -> getsetMech =<< liftIO getTime
                Just oldTo -> do
                  now <- liftIO getTime
                  case now > oldTo of
                    -- expired timeout, use getset mechanism
                    True -> getsetMech now
                    -- someone has it locked
                    False -> return False
    where
      nm' = mkLockName lock nm

      -- this is a reliable mechanism to override an
      -- old/expired/garbled value in the lock. Prevents race
      -- conditions.
      getsetMech now = do
          e <- unwrap $ getset nm' (B.pack . show $ now + to)
          case e of
            -- no old value in there, shouldn't happen in practice
            Nothing -> return True
            -- check that value read is expired
            Just curVal' ->
              case readMay $ B.unpack curVal' of
                -- can't even read, bizarre
                Nothing -> return True
                 -- see if it was expired; if not,
                 -- someone else beat us to the lock
                Just oldTo' -> return $ now > oldTo'




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
bracketRenewable
    :: B.ByteString
    -- ^ namespace for this lock
    -> B.ByteString
    -- ^ Name to lock
    -> Redis a
    -- ^ action to bracket
    -> Redis (Maybe a)
bracketRenewable lock nm action = do
    a <- blocking defBlockPolicy $ acquireLock lock 5 nm'
    case a of
      False -> return Nothing
      True -> do
        res <- action
        releaseLock lock nm'
        return $ Just res
  where
    nm' = nm <> "_lock"


-------------------------------------------------------------------------------
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
blockRenewableLock set lock to nm =
    blocking set $
    acquireRenewableLock lock to nm


-------------------------------------------------------------------------------
-- | Like acquireLock, but for renewable locks.
acquireRenewableLock
    :: B.ByteString
    -- ^ namespace for this lock
    -> Double
    -- ^ timeout in seconds
    -> B.ByteString
    -- ^ Name to lock
    -> Redis Bool
acquireRenewableLock lock to nm =
    fromMaybe False `fmap`
    bracketRenewable lock nm (acquireLock lock to nm)


------------------------------------------------------------------------------
-- | Renews a renewable lock.
renewRenewableLock
    :: B.ByteString
    -- ^ namespace for this lock
    -> Double
    -- ^ timeout in seconds
    -> B.ByteString
    -- ^ Name to lock
    -> Redis Bool
renewRenewableLock lock to nm =
    fmap (fromMaybe False) $ bracketRenewable lock nm $ do
      releaseLock lock nm
      acquireLock lock to nm


-------------------------------------------------------------------------------
-- | Release a renewable lock
releaseRenewableLock
    :: B.ByteString
    -- ^ namespace for this lock
    -> B.ByteString
    -- ^ Name of item to release
    -> Redis Bool
releaseRenewableLock lock nm =
    fmap (maybe False (const True)) $
    bracketRenewable lock nm $
    releaseLock lock nm


-------------------------------------------------------------------------------
mkLockName lock nm = B.intercalate ":" ["_lock", lock, nm]


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


