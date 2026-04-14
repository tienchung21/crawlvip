from curl_cffi import requests
import os
import sys


def _get_env_proxy() -> str:
    return (
        os.environ.get("HTTP_PROXY")
        or os.environ.get("http_proxy")
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("https_proxy")
        or ""
    )


def main():
    env_proxy = _get_env_proxy()
    arg_proxy = sys.argv[1] if len(sys.argv) > 1 else ""
    proxy = arg_proxy or env_proxy

    print("env_proxy:", env_proxy or "(none)")
    print("use_proxy:", proxy or "(none)")

    # Direct IP
    try:
        r = requests.get("https://api.ipify.org?format=json", impersonate="chrome124", timeout=20)
        print("direct_ip:", r.text)
    except Exception as e:
        print("direct_ip_error:", e)

    # Proxy IP
    if proxy:
        try:
            r = requests.get(
                "https://api.ipify.org?format=json",
                impersonate="chrome124",
                proxies={"http": proxy, "https": proxy},
                timeout=20,
            )
            print("proxy_ip:", r.text)
        except Exception as e:
            print("proxy_ip_error:", e)


if __name__ == "__main__":
    main()
