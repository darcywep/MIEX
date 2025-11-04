// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/// @title Fibonacci with persistent storage and event
/// @author ...
/// @notice 计算斐波那契数并将结果存储在链上，每个地址仅保存自己最近一次计算
contract FibonacciStorage {
    /// @dev 存储每个地址最近一次计算的结果
    mapping(address => uint256) public lastResult;

    /// @notice 当计算完成时触发
    /// @param user 调用者地址
    /// @param n 计算的参数 n
    /// @param result 对应的第 n 项结果
    event FibonacciCalculated(address indexed user, uint256 indexed n, uint256 result);

    /// @notice 迭代计算斐波那契数（推荐的链上安全算法）
    /// @param n 第 n 项
    /// @return fib 第 n 项的结果
    function calculate(uint256 n) public returns (uint256 fib) {
        if (n == 0) {
            fib = 0;
        } else if (n == 1) {
            fib = 1;
        } else {
            uint256 a = 0;
            uint256 b = 1;
            for (uint256 i = 2; i <= n; i++) {
                uint256 c = a + b;
                a = b;
                b = c;
            }
            fib = b;
        }

        // 存储计算结果
        lastResult[msg.sender] = fib;

        // 触发事件
        emit FibonacciCalculated(msg.sender, n, fib);

        return fib;
    }

    /// @notice 查询调用者上次计算的结果
    /// @return 上一次的斐波那契值
    function getMyLastResult() public view returns (uint256) {
        return lastResult[msg.sender];
    }

    /// @notice 查询任意地址上次计算的结果（方便外部查询）
    /// @param user 要查询的地址
    /// @return 对应地址最近一次计算结果
    function getUserLastResult(address user) public view returns (uint256) {
        return lastResult[user];
    }
}
