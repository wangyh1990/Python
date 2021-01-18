

def sync_monitor(self):
    day = self.get_argument("day", "")
    # channel = self.get_argument("channel_id", "")
    # status = self.get_argument("status")
    where_day = ""
    if day != "":
        where_day = "and day = '%s'" % day

    sql = "select idx, channel, `day`, recheck_status, local_more_count, dmp_more_count, update_time from post_process_day_job where status in (3, 4, 5) %s" % where_day
    res = db.query(sql, 'db')