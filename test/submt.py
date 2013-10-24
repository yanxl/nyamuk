__author__ = 'apple'
#-*- encoding: utf-8 -*-

import sys
import string
import threading
import time
import logging

from nyamuk import nyamuk
from nyamuk import nyamuk_const as NC
from nyamuk import event


def thread_main(tid):
    global count, mutex
    # get thread name
    threadname = threading.currentThread().getName()

    server = sys.argv[1]
    client_id = '_'.join([sys.argv[2], str(threadname)])
    topic = '_'.join([sys.argv[3], str(tid)])

    print client_id, topic
    start_nyamuk(server, client_id, topic)


def main(num):
    global count, mutex
    threads = []

    count = 0
    mutex = threading.Lock()
    # first create thread objects
    for x in xrange(0, num):
        threads.append(threading.Thread(target=thread_main, args=(x,)))
        # start all threads
    for t in threads:
        t.start()

    for t in threads:
        t.join()

def handle_connack(ev):
    print "CONNACK received"
    rc = ev.ret_code
    if rc == 0:
        print "\tconnection success"
    elif rc == 1:
        print "\tConnection refused : unacceptable protocol version"
    elif rc == 2:
        print "\tConnection refused : identifier rejected"
    elif rc == 3:
        print "\tConnection refused : broker unavailable"
    elif rc == 4:
        print "\tConnection refused : bad username or password"
    elif rc == 5:
        print "\tConnection refused : not authorized"
    else:
        print "\tConnection refused : unknown reason = ", rc

    return rc

def handle_publish(ev):
    msg = ev.msg
    print "PUBLISH_RECEIVED"
    print "\ttopic : " + msg.topic
    if msg.payload is not None:
        print "\tpayload : " + msg.payload

def handle_suback(ev):
    print "SUBACK RECEIVED"
    print "\tQOS Count = ", len(ev.granted_qos)
    print "\tMID = ", ev.mid

def start_nyamuk(server, client_id, topic, username = None, password = None):
    ny = nyamuk.Nyamuk(client_id, username, password, server)
    rc = ny.connect()
    if rc != NC.ERR_SUCCESS:
        print "Can't connect"
        sys.exit(-1)

    rc = ny.subscribe(topic, 0)

    while rc == NC.ERR_SUCCESS:
        ev = ny.pop_event()
        if ev != None:
            if ev.type == NC.CMD_CONNACK:
                handle_connack(ev)
            elif ev.type == NC.CMD_PUBLISH:
                handle_publish(ev)
            elif ev.type == NC.CMD_SUBACK:
                handle_suback(ev)
            elif ev.type == EV_NET_ERR:
                print "Network Error. Msg = ", ev.msg
                sys.exit(-1)
        rc = ny.loop()
        if rc == NC.ERR_CONN_LOST:
            ny.logger.fatal("Connection to server closed")

    ny.logger.info("subscriber exited")

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print "usage    : python submt.py server client_id topic"
        print "example  : python submt.py localhost sub-iwan teknobridges"
        sys.exit(0)

    main(400)
