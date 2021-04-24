# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function, unicode_literals

import os, time
from contextlib import closing
from subprocess import call
from datetime import datetime, timedelta
import configparser
import json
import pickle

from redisworks import Root

from zato.server.service import Service

class LcnSqlRequestToRedis(Service):
    name='Lcn.Sql.To.Redis.Final' 

    def handle(self):
        # Config
        config = configparser.ConfigParser(converters={'list': lambda x: [i.strip() for i in x.split(',')]})
        config.read('/opt/properties/lcn_config.ini')
        # nomBaseOracle = config.get('enregistrement_sms', 'nom_base_oracle')
        nomBaseOracle = 'LCN_PROD'
        nomRedisKeyLcnDaOfferDesc = config.get('enregistrement_sms', 'nom_redis_key_table_lcn_da_offer_desc')
        nomRedisKeyLcnOdsSdp = config.get('enregistrement_sms', 'nom_redis_key_table_lcn_ods_sdp')

        now = datetime.now()
        yesterday = now - timedelta(days=1)

        # LCN_ETL.LCN_DA_OFFER_DESC
        nomFlagLcnDaOfferDesc = """{}_{}""".format(nomRedisKeyLcnDaOfferDesc, yesterday.strftime("%Y%m%d"))
        # Effacant le flag et la valeur d'hier
        self.kvdb.conn.delete(nomFlagLcnDaOfferDesc)
        self.kvdb.conn.delete(nomRedisKeyLcnDaOfferDesc)
        query = """ SELECT ID_DA,
                           DA,
                           OFFER_ID,
                           DURATION,
                           UNIT_DURATION
                    FROM LCN_ETL.LCN_DA_OFFER_DESC
                        WHERE IS_RENEWAL='Y' """
        input_required = {"query": query, 
                          "out_name": nomBaseOracle, 
                          "table_name": "LCN_ETL.LCN_DA_OFFER_DESC", 
                          "output_dict_name": nomRedisKeyLcnDaOfferDesc, 
                          "output_dict_key": "offer_id",
                          "operation_name": ""}
        self.invoke("Sql.To.Redis.Service.Final", input_required)
        nomFlagLcnDaOfferDesc = """{}_{}""".format(nomRedisKeyLcnDaOfferDesc, now.strftime("%Y%m%d"))
        # Initialisation (delete) pour aujourd hui
        self.kvdb.conn.delete(nomFlagLcnDaOfferDesc)
        self.kvdb.conn.set(nomFlagLcnDaOfferDesc, "true")


        # LCN_ETL.LCN_ODS_SDP
        nomFlagLcnOdsSdp = """{}_{}""".format(nomRedisKeyLcnOdsSdp, yesterday.strftime("%Y%m%d"))
        # Effacant le flag et la valeur d'hier
        self.kvdb.conn.delete(nomFlagLcnOdsSdp)
        self.kvdb.conn.delete(nomRedisKeyLcnOdsSdp)
        query = """ SELECT SUBSCRIBER,
                           ID_LANGUAGE,
                           ACTIVATION_DATE,
                           GP1_DATE,
                           GP2_DATE
                    FROM LCN_ETL.LCN_ODS_SDP
                        WHERE ACTIVATION_DATE IS NOT NULL"""
        input_required = {"query": query, 
                          "out_name": nomBaseOracle, 
                          "table_name": "LCN_ETL.LCN_ODS_SDP", 
                          "output_dict_name": nomRedisKeyLcnOdsSdp, 
                          "output_dict_key": "subscriber",
                          "operation_name": ""}
        self.invoke("Sql.To.Redis.Service.Final", input_required)
        nomFlagLcnOdsSdp = """{}_{}""".format(nomRedisKeyLcnOdsSdp, now.strftime("%Y%m%d"))
        # Initialisation (delete) pour aujourd hui
        self.kvdb.conn.delete(nomFlagLcnOdsSdp)
        self.kvdb.conn.set(nomFlagLcnOdsSdp, "true")



