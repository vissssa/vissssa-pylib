import tenacity

retryer = tenacity.Retrying(stop=tenacity.stop_after_attempt(3),
                            wait=tenacity.wait_incrementing(
                                start=0.01, increment=0.05),
                            retry=tenacity.retry_if_exception_type(Exception),
                            reraise=True)
