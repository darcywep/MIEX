// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/// @title MinimalSmallBank - 极简测试版 ERC20 风格银行合约
/// @notice 仅用于测试逻辑，每个地址一个账户，只支持存款、取款、转账
contract MinimalSmallBank {

    // ========================
    // 数据结构
    // ========================

    mapping(address => uint256) private balances;      // 每个账户的余额（地址 → 金额）

    // ========================
    // 防重入机制
    // ========================

    uint256 private constant _NOT_ENTERED = 1;         // 常量：未进入状态
    uint256 private constant _ENTERED = 2;             // 常量：已进入状态
    uint256 private _status = _NOT_ENTERED;            // 当前执行状态初始化为未进入

    modifier nonReentrant() {                          // 定义防重入修饰器
        require(_status != _ENTERED, "Reentrant call");// 确保未在执行中
        _status = _ENTERED;                            // 标记为已进入
        _;                                             // 执行函数体
        _status = _NOT_ENTERED;                        // 执行完恢复状态
    }

    // ========================
    // 事件（方便测试查看）
    // ========================

    event Deposit(address indexed user, uint256 amount);  // 存款事件
    event Withdraw(address indexed user, uint256 amount); // 取款事件
    event Transfer(address indexed from, address indexed to, uint256 amount); // 转账事件

    // ========================
    // 基础功能
    // ========================

    function openAccount(uint256 initialBalance) external { // 开户函数，可传入初始金额
        require(balances[msg.sender] == 0, "Already opened"); // 不允许重复开户
        balances[msg.sender] = initialBalance;               // 设置初始余额
        emit Deposit(msg.sender, initialBalance);             // 触发事件
    }

    function deposit(uint256 amount) external nonReentrant {  // 存款函数
        require(amount > 0, "Deposit > 0");                  // 检查金额大于 0
        balances[msg.sender] += amount;                      // 增加余额
        emit Deposit(msg.sender, amount);                    // 触发事件
    }

    function withdraw(uint256 amount) external nonReentrant { // 取款函数
        require(amount > 0, "Withdraw > 0");                 // 检查金额大于 0
        uint256 bal = balances[msg.sender];                  // 获取当前余额
        require(bal >= amount, "Not enough balance");        // 检查余额足够
        balances[msg.sender] = bal - amount;                 // 扣减余额
        emit Withdraw(msg.sender, amount);                   // 触发事件
    }

    function transfer(address to, uint256 amount) external nonReentrant { // 转账函数
        require(to != address(0), "Invalid target");          // 检查目标地址合法
        require(amount > 0, "Transfer > 0");                  // 检查金额大于 0
        uint256 senderBal = balances[msg.sender];             // 获取发送者余额
        require(senderBal >= amount, "Not enough balance");   // 检查余额足够
        balances[msg.sender] = senderBal - amount;            // 扣减发送者余额
        balances[to] += amount;                               // 增加接收者余额
        emit Transfer(msg.sender, to, amount);                // 触发事件
    }

    function getBalance(address who) external view returns (uint256) { // 查询余额
        return balances[who];                                // 返回余额
    }

    function closeAccount() external nonReentrant {           // 关闭账户
        uint256 bal = balances[msg.sender];                   // 获取余额
        require(bal > 0, "Nothing to close");                 // 必须有余额
        balances[msg.sender] = 0;                             // 清空余额
        emit Withdraw(msg.sender, bal);                       // 触发事件（视为取款）
    }
}
