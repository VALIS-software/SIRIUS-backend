#!/usr/bin/env python

def bed_stream(cursor):
    return iter((d['chromid'], d['start'], d['end']) for d in cursor)

