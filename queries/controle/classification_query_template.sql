WITH
    latest_session AS (
    SELECT
        * EXCEPT(rn)
    FROM (
        SELECT
        ROW_NUMBER() OVER(PARTITION BY clientid ORDER BY visitId DESC) AS rn,
        clientId,
        visitNumber,
        channelGrouping,
        IF(device.browser NOT IN ('Chrome', 'Safari', 'Firefox', 'Android Webview', 'Edge'), 'Others', device.browser) as browser,
        device.deviceCategory,
        IF(device.operatingSystem NOT IN('Android', 'iOS', 'Windows', 'Macintosh', 'Linux'), 'Others', device.operatingSystem ) AS operatingSystem,
        CASE
          WHEN device.isMobile = TRUE THEN 1
          ELSE 0
        END AS isMobile,
        geoNetwork.region,
        geoNetwork.city,
        FROM
        `{table}`
        WHERE
        _TABLE_SUFFIX BETWEEN '{classification_start_date}' AND '{classification_end_date}'
        AND clientId IS NOT NULL
        )
    WHERE
        rn = 1 
    ),

    -- New Block to Select First Channel
    first_session AS (
    SELECT
        * EXCEPT(rn)
    FROM (
        SELECT
        ROW_NUMBER() OVER(PARTITION BY clientid ORDER BY visitId ASC) AS rn,
        clientId,
        channelGrouping
        FROM
          `mynt-tim-brazil.50835115.ga_sessions_*` 
        WHERE
          _TABLE_SUFFIX BETWEEN '{classification_start_date}' AND '{classification_end_date}'
        AND clientId IS NOT NULL)
    WHERE
        rn = 1 ),
    
    totals as (
    SELECT
        clientId,
        COUNT(DISTINCT channelGrouping) AS num_channels,
        COUNT(DISTINCT CONCAT(trafficSource.source, " / ", trafficSource.medium)) AS num_sources,
        COUNT(DISTINCT date) AS num_days,
        COUNT(DISTINCT device.deviceCategory) AS num_devices,
        SUM(totals.visits) AS visits,
        SUM(totals.pageviews) AS pageviews,
        SUM(totals.hits) AS hits,
        SUM(totals.timeonsite) AS timeonsite,
        SUM(totals.bounces) AS bounces,
        SUM(CASE WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime) AT TIME ZONE "America/Sao_Paulo") IN (5,6,7,8,9,10) THEN 1 ELSE 0 END) AS morning_visits,
        SUM(CASE WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime) AT TIME ZONE "America/Sao_Paulo") IN (11,12,13,14,15,16) THEN 1 ELSE 0 END) AS daytime_visits,
        SUM(CASE WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime) AT TIME ZONE "America/Sao_Paulo") IN (17,18,19,20,21,22) THEN 1 ELSE 0 END) AS evening_visits,
        SUM(CASE WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime) AT TIME ZONE "America/Sao_Paulo") IN (23,24,0,1,2,3,4) THEN 1 ELSE 0 END) AS midnight_visits,
        SUM(totals.transactions) AS trasactions,
        SUM(totals.totalTransactionRevenue) / 100000 AS revenue
    FROM
      `{table}`
    WHERE
      _TABLE_SUFFIX BETWEEN '{classification_start_date}' AND '{classification_end_date}'
    AND clientId IS NOT NULL   
    GROUP BY 1
    ),

    session_hits as (
    SELECT
        clientId,
        SUM(CASE WHEN h.eventinfo.eventcategory = 'tim-controle-lp' AND h.eventInfo.eventLabel ='contratar-agora!' THEN 1 ELSE 0 END) AS Event01,
        SUM(CASE WHEN h.eventinfo.eventcategory = 'tim-controle-conheca-os-planos-e-promocoes' AND h.eventInfo.eventLabel ='contratar-agora' THEN 1 ELSE 0 END) AS Event02,
        SUM(CASE WHEN h.eventinfo.eventcategory = 'tim-controle-conheca-os-planos-e-promocoes' AND h.eventInfo.eventAction='tim-controle-planos' AND h.eventInfo.eventLabel ='mais-planos-controle' THEN 1 ELSE 0 END) AS Event03,
        SUM(CASE WHEN h.eventinfo.eventcategory = 'tim-controle-modal' AND h.eventInfo.eventLabel ='portabilidade-migracao' THEN 1 ELSE 0 END) AS Event04,
        SUM(CASE WHEN h.eventinfo.eventcategory = 'tim-controle-conheca-os-planos-e-promocoes' AND h.eventInfo.eventLabel ='contratar-agora' THEN 1 ELSE 0 END) AS Event05,
        SUM(CASE WHEN h.eventinfo.eventcategory = 'tim-controle-light-planos-tim' AND h.eventInfo.eventAction='tim-controle-light' AND h.eventInfo.eventLabel ='contratar-agora' THEN 1 ELSE 0 END) AS Event06,
        SUM(CASE WHEN h.eventinfo.eventcategory = 'menu-principal' AND h.eventInfo.eventAction='menu-planos' AND h.eventInfo.eventLabel ='tim-controle' THEN 1 ELSE 0 END) AS Event07,
        SUM(CASE WHEN h.eventinfo.eventcategory = 'home-para-voce' AND h.eventInfo.eventAction='banner-superior-item' AND h.eventInfo.eventLabel ='controle' THEN 1 ELSE 0 END) AS Event08,
        SUM(CASE WHEN h.eventinfo.eventcategory = 'home-para-voce-banner-central' THEN 1 ELSE 0 END) AS Event09,
        SUM(CASE WHEN h.eventinfo.eventcategory = 'menu-principal' AND h.eventInfo.eventAction='menu-planos' AND h.eventInfo.eventLabel ='controle' THEN 1 ELSE 0 END) AS Event10,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='dados-pessoais' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event11,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='criar-cadastro' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event12,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='endereco' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event13,
        SUM(CASE WHEN h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event14,
        SUM(CASE WHEN h.eventinfo.eventcategory = 'ecommerce-chatbot' AND h.eventInfo.eventAction='adesao' AND UPPER(h.eventInfo.eventLabel) LIKE '%CONTROLE%' THEN 1 ELSE 0 END) AS Event15,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='pagamento-fatura-impressa' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event16,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='pagamento-express-impressa' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event17,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='pagamento-debito-impressa' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event18,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='pagamento-fatura-digital' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event19,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='pagamento-express-digital' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event20,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='pagamento-debito-digital' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event21,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='tim-negocia' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event22,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='sms' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event23,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='oferta-pre' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event24,
        SUM(CASE WHEN UPPER(h.eventinfo.eventcategory) LIKE '%CONTROLE%' AND h.eventInfo.eventAction='oferta-express' AND h.eventInfo.eventLabel ='sucesso' THEN 1 ELSE 0 END) AS Event25,
    FROM
        `{table}` t, UNNEST(hits) h
    WHERE
        _TABLE_SUFFIX BETWEEN '{classification_start_date}' AND '{classification_end_date}'
        AND clientId IS NOT NULL
    GROUP BY 1),

    joined as(
    SELECT
    sh.clientid,
    fs.channelgrouping AS first_channel,
    ls.channelgrouping AS last_channel,
    ls.browser,
    ls.deviceCategory,
    ls.operatingSystem,
    ls.region,
    ls.city,
    ls.visitnumber AS current_visit,
    IFNULL(SUM(t.num_channels), 0) AS num_channels,
    IFNULL(SUM(t.num_sources), 0) AS num_sources,
    IFNULL(SUM(t.num_days), 0) AS num_days,
    IFNULL(SUM(t.num_devices), 0) AS num_devices,
    IFNULL(SUM(t.visits), 0) AS total_visits,
    IFNULL(SUM(t.pageviews), 0) AS total_pageviews,
    IFNULL(SUM(t.hits), 0) AS total_hits,
    IFNULL(SUM(t.timeonsite), 0) AS total_timeonsite,
    IFNULL(SUM(t.bounces), 0) AS total_bounces,
    IFNULL(SUM(t.morning_visits), 0) AS total_morning_visits,
    IFNULL(SUM(t.daytime_visits), 0) AS total_daytime_visits,
    IFNULL(SUM(t.evening_visits), 0) AS total_evening_visits,
    IFNULL(SUM(t.midnight_visits), 0) AS total_midnight_visits,
    IFNULL(SUM(t.trasactions), 0) AS total_trasactions,

    SUM(Event01) AS Event01,
    SUM(Event02) AS Event02,
    SUM(Event03) AS Event03,
    SUM(Event04) AS Event04,
    SUM(Event05) AS Event05,
    SUM(Event06) AS Event06,
    SUM(Event07) AS Event07,
    SUM(Event08) AS Event08,
    SUM(Event09) AS Event09,
    SUM(Event10) AS Event10,
    SUM(Event11) AS Event11,
    SUM(Event12) AS Event12,
    SUM(Event13) AS Event13,
    SUM(Event14) AS Event14,
    SUM(Event15) AS Event15,
    SUM(Event16) AS Event16,
    SUM(Event17) AS Event17,
    SUM(Event18) AS Event18,
    SUM(Event19) AS Event19,
    SUM(Event20) AS Event20,
    SUM(Event21) AS Event21,
    SUM(Event22) AS Event22,
    SUM(Event23) AS Event23,
    SUM(Event24) AS Event24,
    SUM(Event25) AS Event25
    FROM
    session_hits sh 
    JOIN latest_session ls ON sh.clientid = ls.clientid
    JOIN first_session fs ON sh.clientid = fs.clientid
    LEFT OUTER JOIN totals t ON sh.clientid = t.clientid
    GROUP BY 1,2,3,4,5,6,7,8,9
    )
    
    SELECT * FROM joined