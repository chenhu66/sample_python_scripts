import time

def my_task(x, task_id=None):
    """这是你的实际任务函数，可以任意复杂"""
    if task_id is not None:
        print(f"[任务{task_id}] my_task开始处理: {x}")
    else:
        print(f"my_task开始处理: {x}")
    
    time.sleep(x * 0.1)
    
    # 调用子函数
    result1 = helper_function_a(x)
    print(f"  子函数A返回: {result1}")
    
    result2 = helper_function_b(result1)
    print(f"  子函数B返回: {result2}")
    
    final = result2 * 2
    print(f"my_task完成，最终结果: {final}")
    
    return final

def helper_function_a(y):
    """子函数A"""
    print(f"    [helper_a] 计算 {y} 的平方")
    return y ** 2

def helper_function_b(z):
    """子函数B"""
    print(f"    [helper_b] 计算 {z} 加 100")
    return z + 100

import sys
import os

# 导入模块
import multiprocess_print_in_order as mp_pio

def main():
    """主程序入口"""
    # 测试数据
    test_data = range(32)

    print("🚀 主程序启动")
    print("正在导入任务函数和多进程框架...")

    # 运行并行任务
    all_results = mp_pio.safe_parallel_ordered(my_task, test_data, num_workers=4)

    # 展示结果
    print("\n" + "=" * 60)
    print("📊 任务执行结果汇总:")
    print("=" * 60)

    print(all_results)

    # 测试数据
    test_data = range(32,64)

    print("🚀 主程序启动")
    print("正在导入任务函数和多进程框架...")

    # 运行并行任务
    all_results = mp_pio.safe_parallel_ordered(my_task, test_data, num_workers=4)

    # 展示结果
    print("\n" + "=" * 60)
    print("📊 任务执行结果汇总:")
    print("=" * 60)

    print(all_results)

if __name__ == "__main__":
    # 这是关键：确保只在主模块中运行
    main()
