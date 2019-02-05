wrk.method = "POST"
wrk.body = "{\"message\":\"" .. ("."):rep(9500) .. "\"}"
wrk.headers["DNT"] = "\xff"
wrk.headers["X-Pingsender-Version"] = "\xbd"
wrk.headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:63.0)" ..
    " Gecko/20100101 Firefox/63.0"
