from proxy import MAX_ATTEMPTS, MAX_CONCURRENCY


def test_limits_are_positive() -> None:
    assert MAX_ATTEMPTS > 0 and MAX_CONCURRENCY > 0
