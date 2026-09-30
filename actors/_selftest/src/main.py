import os, time
from actorhub_sdk import get_input, push_data, log
i = get_input(); log("selftest start", i)
push_data({"declared_secret_seen": bool(os.environ.get("SELFTEST_SECRET")), "undeclared_secret_seen": bool(os.environ.get("SELFTEST_UNDECLARED"))})
time.sleep(i.get("sleep", 0))
if i.get("fail"): raise RuntimeError("selftest requested failure")
push_data({"done": True}); log("selftest end")
