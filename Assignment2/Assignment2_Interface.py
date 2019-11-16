#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()


def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    cur = openconnection.cursor()
    file = open(outputPath, "w+")
    # For Range_partitions table
    cur.execute("select count(*) from information_schema.tables where table_name like 'rangeratingspart%'")
    res = cur.fetchone()
    res = res[0]
    for i in range(0, res):
        table = 'RangeRatingsPart' + str(i)
        cur.execute("select * from " + table + " where rating>=" + str(ratingMinValue) + " and rating<=" + str(ratingMaxValue))
        ans = cur.fetchall()
        for rows in ans:
            file.write(str(table) + ',' + str(rows[0]) + ',' + str(rows[1]) + ',' + str(rows[2]) + '\n')

    # For RoundRobin_partitions table
    cur.execute("select count(*) from information_schema.tables where table_name like 'roundrobinratingspart%'")
    res = cur.fetchone()
    res = res[0]
    for i in range(0, res):
        table = 'RoundRobinRatingsPart' + str(i)
        cur.execute("select * from " + table + " where rating>=" + str(ratingMinValue) + " and rating<=" + str(ratingMaxValue))
        ans2 = cur.fetchall()
        for rows in ans2:
            file.write(str(table) + ',' + str(rows[0]) + ',' + str(rows[1]) + ',' + str(rows[2]) + '\n')
    file.close()
    cur.close()
    openconnection.commit()

def PointQuery(ratingValue, openconnection, outputPath):
    cur = openconnection.cursor()
    file = open(outputPath, "w+")
    # For Range_partitions table
    cur.execute("select count(*) from information_schema.tables where table_name like 'rangeratingspart%'")
    res = cur.fetchone()
    res = res[0]
    for i in range(0, res):
        table = 'RangeRatingsPart' + str(i)
        cur.execute("select * from " + table + " where rating=" + str(ratingValue))
        ans = cur.fetchall()
        for rows in ans:
            file.write(str(table) + ',' + str(rows[0]) + ',' + str(rows[1]) + ',' + str(rows[2]) + '\n')

    # For RoundRobinPartitions
    cur.execute("select count(*) from information_schema.tables where table_name like 'roundrobinratingspart%'")
    res = cur.fetchone()
    res = res[0]
    for i in range(0, res):
        table = 'RoundRobinRatingsPart' + str(i)
        cur.execute("select * from " + table + " where rating =" + str(ratingValue))
        ans2 = cur.fetchall()
        for rows in ans2:
            file.write(str(table) + ',' + str(rows[0]) + ',' + str(rows[1]) + ',' + str(rows[2]) + '\n')
    file.close()
    cur.close()
    openconnection.commit()

