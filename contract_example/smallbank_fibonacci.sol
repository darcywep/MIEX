// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @title MinimalSmallBank - 带 Fibonacci(递归+迭代) 的极简银行合约
/// @author darcywep
/// @notice 每个地址一个账户，可存取款、转账，并支持基于余额的 Fibonacci 计算更新
contract MinimalSmallBank {
    // ========================
    // 数据结构
    // ========================

    mapping(address => uint256) private balances;

    // ========================
    // 防重入机制
    // ========================

    uint256 private constant _NOT_ENTERED = 1;
    uint256 private constant _ENTERED = 2;
    uint256 private _status = _NOT_ENTERED;

    modifier nonReentrant() {
        require(_status != _ENTERED, "Reentrant call");
        _status = _ENTERED;
        _;
        _status = _NOT_ENTERED;
    }

    // ========================
    // 事件
    // ========================

    event Deposit(address indexed user, uint256 amount);
    event Withdraw(address indexed user, uint256 amount);
    event Transfer(address indexed from, address indexed to, uint256 amount);
    event FibonacciCalculated(address indexed user, uint256 indexed n, uint256 result, bool recursive);

    // ========================
    // 基础功能
    // ========================

    function openAccount(uint256 initialBalance) external {
        require(balances[msg.sender] == 0, "Already opened");
        balances[msg.sender] = initialBalance;
        emit Deposit(msg.sender, initialBalance);
    }

    function deposit(uint256 amount) external nonReentrant {
        require(amount > 0, "Deposit > 0");
        balances[msg.sender] += amount;
        emit Deposit(msg.sender, amount);
    }

    function withdraw(uint256 amount) external nonReentrant {
        require(amount > 0, "Withdraw > 0");
        uint256 bal = balances[msg.sender];
        require(bal >= amount, "Not enough balance");
        balances[msg.sender] = bal - amount;
        emit Withdraw(msg.sender, amount);
    }

    function transfer(address to, uint256 amount) external nonReentrant {
        require(amount > 0, "Transfer > 0");
        uint256 senderBal = balances[msg.sender];
        require(senderBal >= amount, "Not enough balance");
        balances[msg.sender] = senderBal - amount;
        balances[to] += amount;
        emit Transfer(msg.sender, to, amount);
    }

    function getBalance(address who) external view returns (uint256) {
        return balances[who];
    }

    function closeAccount() external nonReentrant {
        uint256 bal = balances[msg.sender];
        require(bal > 0, "Nothing to close");
        balances[msg.sender] = 0;
        emit Withdraw(msg.sender, bal);
    }

    // ========================
    // Fibonacci 功能
    // ========================

    /// @notice 内部递归版 Fibonacci
    /// @dev 限制 n <= 30，防止栈溢出
    function _fibonacciRecursive(uint256 n) internal pure returns (uint256) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return _fibonacciRecursive(n - 1) + _fibonacciRecursive(n - 2);
    }

    /// @notice 内部迭代版 Fibonacci
    function _fibonacciIterative(uint256 n) internal pure returns (uint256) {
        if (n == 0) return 0;
        if (n == 1) return 1;

        uint256 a = 0;
        uint256 b = 1;
        for (uint256 i = 2; i <= n; i++) {
            uint256 c = a + b;
            a = b;
            b = c;
        }
        return b;
    }

    /// @notice 根据账户余额计算 Fibonacci(n)，递归/迭代可选
    /// @param user 要操作的账户地址
    /// @param n Fibonacci 参数
    /// @param recursive 是否使用递归计算（true=递归，false=迭代）
    function fibonacciCalculate(address user, uint256 n, bool recursive) external nonReentrant {
        require(n > 0, "n must > 0");

        uint256 fib;
        if (recursive) {
            fib = _fibonacciRecursive(n);
        } else {
            fib = _fibonacciIterative(n);
        }

        emit FibonacciCalculated(user, n, fib, recursive);

        // 更新余额为 (fib / 2) * 1e18
        uint256 newBalance = balances[user] + fib/2;
        balances[user] = newBalance;

        emit Deposit(user, newBalance);
    }
}
