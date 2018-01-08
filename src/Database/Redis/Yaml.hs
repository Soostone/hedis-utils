{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# LANGUAGE OverloadedStrings #-}
-- | Orphaned instance for ConnnectInfo for loading from YAML.
module Database.Redis.Yaml where

-------------------------------------------------------------------------------
import           Data.Yaml
import           Database.Redis as H
-------------------------------------------------------------------------------


instance FromJSON H.ConnectInfo where
    parseJSON = withObject "ConnectInfo" $ \v -> do
      host <- v .: "host"
      port <- v .: "port"
      n <- v .: "max-conn"
      to <- v .: "timeout"
      return $ H.defaultConnectInfo
        { H.connectHost = host
        , H.connectPort = H.PortNumber (fromIntegral (port::Int))
        , H.connectMaxConnections = n
        , H.connectMaxIdleTime = realToFrac (to::Double)}


