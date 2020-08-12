--
-- NDT7 upload data in standard columns plus additional annotations.
-- This contributes one portion of the data used by MLab Unified Standard Views.
--
-- This view is only intended to accessed by a MLab Standard views: breaking changes
-- here will be offset by changes to the Published Standard views.
--
-- Anything here not visible in a standard view is subject to breaking changes.
--

WITH ndt7uploads AS (
  SELECT *,
  raw.Upload.ClientMeasurements[SAFE_ORDINAL(ARRAY_LENGTH(raw.Upload.ServerMeasurements))] AS lastSample,
# (raw.Upload.Error != "") AS IsErrored,  -- TODO NOT IN NDT7
  False AS IsErrored,  -- TODO MISSING?
  TIMESTAMP_DIFF(raw.Upload.EndTime, raw.Upload.StartTime, MICROSECOND) AS connection_duration
  FROM   `mlab-oti.raw_ndt.ndt7`
  -- Limit to valid S2C results
  WHERE raw.Upload IS NOT NULL  -- TODO CHECK before Publication
  AND raw.Upload.ServerMeasurements IS NOT NULL   -- TODO CHECK before Publication
  AND ARRAY_LENGTH(raw.Upload.ServerMeasurements) > 0 -- TODO CHECK before Publication
  AND raw.Upload.UUID IS NOT NULL
  AND raw.Upload.UUID NOT IN ( '', 'ERROR_DISCOVERING_UUID' )
),

PreCleanNDT7 AS (
  SELECT
    uploads.id, uploads.date, uploads.a, uploads.IsErrored, uploads.lastsample,
    uploads.connection_duration, uploads.raw,
    annotation.Client, annotation.Server,
    -- Receiver side can not compute IsCongested
    -- Receiver side can not directly compute IsBloated
    ( raw.ClientIP IN
         -- TODO(m-lab/etl/issues/893): move to parser configuration.
        ( "35.193.254.117", -- script-exporter VMs in GCE, sandbox.
          "35.225.75.192", -- script-exporter VM in GCE, staging.
          "35.192.37.249", -- script-exporter VM in GCE, oti.
          "23.228.128.99", "2605:a601:f1ff:fffe::99", -- ks addresses.
          "45.56.98.222", "2600:3c03::f03c:91ff:fe33:819", -- eb addresses.
          "35.202.153.90", "35.188.150.110" -- Static IPs from GKE VMs for e2e tests.
        )
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.ServerIP),
                8) = NET.IP_FROM_STRING("10.0.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.ServerIP),
                12) = NET.IP_FROM_STRING("172.16.0.0"))
      OR (NET.IP_TRUNC(NET.SAFE_IP_FROM_STRING(raw.ServerIP),
                16) = NET.IP_FROM_STRING("192.168.0.0"))
    ) AS IsOAM,  -- Data is not from valid clients
    uploads.parser AS NDT7parser,
    annotation.parser AS Annoparser
  FROM
    -- Use a left join to allow NDT test without matching annotations. TODO test
    ndt7uploads AS uploads
    LEFT JOIN `mlab-oti.raw_ndt.annotation` AS annotation
    ON
	uploads.date = annotation.date AND
	uploads.id = annotation.id
),

NDT7UploadModels AS (
  SELECT
    id,
    date,
    STRUCT (
      a.UUID,
      a.TestTime,
      a.CongestionControl,
      a.MeanThroughputMbps,
      a.MinRTT * 1000.0, -- TODO issue
      Null AS LossRate  -- Receiver can not disambiguate reordering and loss
    ) AS a,
    STRUCT (
     "ndt7" AS _DataSilo -- TODO THIS WILL CHANGE
    ) AS node,
    -- Struct filter has predicates for various cleaning assumptions
    STRUCT (
      (  -- No S2C test for client vs network bottleneck
        NOT IsOAM AND NOT IsErrored
        AND lastSample.TCPInfo.BytesReceived IS NOT NULL
        AND lastSample.TCPInfo.BytesReceived >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
      ) AS IsValidBest,
      (  -- No S2C test for client vs network bottleneck
        NOT IsOAM AND NOT IsErrored
        AND lastSample.TCPInfo.BytesReceived IS NOT NULL
        AND lastSample.TCPInfo.BytesReceived >= 8192
        AND connection_duration BETWEEN 9000000 AND 60000000
      ) AS IsValid2019  -- Same as row_valid_best
    ) AS filter,
    -- NOTE: standard columns for views exclude the parseInfo struct because
    -- multiple tables are used to create a derived view. Users that want the
    -- underlying parseInfo values should refer to the corresponding tables
    -- using the shared UUID.
    STRUCT (
      raw.ClientIP AS IP,
      raw.ClientPort AS Port,
      STRUCT (  -- Map new geo into older production geo
      	     client.Geo.ContinentCode, -- aka continent_code,
	     client.Geo.CountryCode, -- aka country_code,
	     client.Geo.CountryCode3, -- aka country_code3,
	     client.Geo.CountryName, -- aka country_name,
	     client.Geo.Region, -- aka region,
	     -- client.Geo. Subdivision1ISOCode -- OMITED
	     -- client.Geo. Subdivision1Name -- OMITED
	     -- client.Geo.Subdivision2ISOCode -- OMITED
	     -- client.Geo.Subdivision2Name -- OMITED
	     client.Geo.MetroCode, -- aka metro_code,
	     client.Geo.City, -- aka city,
	     client.Geo.AreaCode, -- aka area_code,
	     client.Geo.PostalCode, -- aka postal_code,
	     client.Geo.Latitude, -- aka latitude,
	     client.Geo.Longitude, -- aka longitude,
	     client.Geo.AccuracyRadiusKm -- aka radius
	     -- client.Geo.Missing -- Future
      ) AS Geo,
      STRUCT(
        -- NOTE: Omit the NetBlock field because neither web100 nor ndt5 tables
        -- includes this information yet.
        -- NOTE: Select the first ASN b/c standard columns defines a single field.
        CAST (Client.Network.Systems[OFFSET(0)].ASNs[OFFSET(0)] AS STRING) AS ASNumber
      ) AS Network
    ) AS client,
    STRUCT (
      raw.ServerIP AS IP,
      raw.ServerPort AS Port,
      REGEXP_EXTRACT(NDT7parser.ArchiveURL,
            'mlab[1-4]-([a-z][a-z][a-z][0-9][0-9t])') AS Site, -- e.g. lga02
      REGEXP_EXTRACT(NDT7parser.ArchiveURL,
            '(mlab[1-4])-[a-z][a-z][a-z][0-9][0-9t]') AS Machine, -- e.g. mlab1
      STRUCT (  -- Map new geo into older production geo
      	     client.Geo.ContinentCode, -- aka continent_code,
	     client.Geo.CountryCode, -- aka country_code,
	     client.Geo.CountryCode3, -- aka country_code3,
	     client.Geo.CountryName, -- aka country_name,
	     client.Geo.Region, -- aka region,
	     -- client.Geo. Subdivision1ISOCode -- OMITED
	     -- client.Geo. Subdivision1Name -- OMITED
	     -- client.Geo.Subdivision2ISOCode -- OMITED
	     -- client.Geo.Subdivision2Name -- OMITED
	     client.Geo.MetroCode, -- aka metro_code,
	     client.Geo.City, -- aka city,
	     client.Geo.AreaCode, -- aka area_code,
	     client.Geo.PostalCode, -- aka postal_code,
	     client.Geo.Latitude, -- aka latitude,
	     client.Geo.Longitude, -- aka longitude,
	     client.Geo.AccuracyRadiusKm -- aka radius
	     -- client.Geo.Missing -- Future
      ) AS Geo,
      STRUCT(
        CAST (Server.Network.Systems[OFFSET(0)].ASNs[OFFSET(0)] AS STRING) AS ASNumber
      ) AS Network
    ) AS server,
    date AS test_date,
#    PreCleanNDT7 AS _internal202008  -- Not stable and subject to breaking changes

  FROM PreCleanNDT7
)

SELECT * FROM NDT7UploadModels
