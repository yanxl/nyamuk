'''
Yanxl multi threads publisher example
'''
import sys
import string
import threading
import time

from nyamuk import nyamuk
import nyamuk.nyamuk_const as NC
from nyamuk import event

def thread_main(pub_time, tid):
    global count, mutex
    # get thread name
    threadname = threading.currentThread().getName()

    server = sys.argv[1]
    name = ' '.join([sys.argv[2], str(threadname)])
    topic = '_'.join([sys.argv[3], str(tid)])

    for x in xrange(0, int(pub_time)):
        # get lock
        mutex.acquire()
        count = count + 1
        # release lock
        mutex.release()
        print threadname, x, count
        payload = ' '.join([sys.argv[4], str(threadname), str(count)])
        start_nyamuk(server, name, topic, payload)
        time.sleep(1)


def main(num):
    global count, mutex
    threads = []

    count = 0
    mutex = threading.Lock()
    # first create thread objects
    for x in xrange(0, num):
        threads.append(threading.Thread(target=thread_main, args=(100, x)))
    # start all threads
    for t in threads:
        t.start()

    for t in threads:
        t.join()


def handle_connack(ev):
    print "CONNACK received"
    rc = ev.ret_code
    if rc == NC.CONNECT_ACCEPTED:
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

def start_nyamuk(server, client_id, topic, payload):
    ny = nyamuk.Nyamuk(client_id, server = server)
    rc = ny.connect()
    if rc != NC.ERR_SUCCESS:
        print "Can't connect"
        sys.exit(-1)

    index = 0

    while rc == NC.ERR_SUCCESS:
        ev = ny.pop_event()
        if ev != None and ev.type == NC.CMD_CONNACK:
            ret_code = handle_connack(ev)
            if ret_code == NC.CONNECT_ACCEPTED:
                print "publishing payload"
                ny.publish(topic, payload)
                rc = ny.loop()
                return

        rc = ny.loop()

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print "usage   : python submq.py server name topic payload"
        print "example : python submq.py localhost sub-iwan teknobridges HaloMqtt"
        sys.exit(0)

    main(400)
