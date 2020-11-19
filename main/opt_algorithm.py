import subprocess


def opt_alg(opl_path, mod_path, dat_path, timeout):
    proc = subprocess.Popen(
        [opl_path, mod_path, dat_path],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate(timeout=timeout)
    opl = out.decode('utf-8')
    data_err = err.decode('utf-8')
    opl = opl[opl.find("["):opl.find("\n<<< post process")]
    return {"opl": opl, "err": data_err}


if __name__ == "__main__":
    res = opt_alg("./oplrun", "./foodhwy1.mod", "./failed_dats/20190816-181555263563.dat")
    print(res)
