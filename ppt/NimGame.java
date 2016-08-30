
//Memory Limit Exceeded
public boolean canWinNim(int n) {
	//Need not to open such a big array
    boolean[] result = new boolean[n + 1];
    if (n == 1 || n == 2 || n == 3) return true;
    result[1] = true;
    result[2] = true;
    result[3] = true;
        
    for (int i = 4; i <= n; i++) {
        if (result[i - 1] && result[i - 2] && result[i - 3]) result[i] = false;
        else result[i] = true;
    }
    return result[n];
}

//Time Limit Exceeded
public boolean canWinNim(int n) {
    if (n == 1 || n == 2 || n == 3) return true;
    boolean f1 = true;
    boolean f2 = true;
    boolean f3 = true;
        
    for (int i = 4; i <= n; i++) {
        boolean tmp = true;
        if (f1 && f2 && f3) tmp = false;
        f1 = f2;
        f2 = f3;
        f3 = tmp;
    }
    return f3;
}

public boolean canWinNim(int n) {
    return !(n % 4 == 0);
}