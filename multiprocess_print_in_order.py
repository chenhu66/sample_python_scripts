import multiprocessing as mp
import time
import sys
from io import StringIO
import threading
from queue import Empty
from contextlib import redirect_stdout

def worker_with_output(task_func, idx, item, output_queue):
    """
    工作进程：捕获本进程内所有print（包括深层子函数）
    """
    # 1. 创建字符串缓冲区来捕获输出
    output_buffer = StringIO()
    
    # 2. 使用上下文管理器重定向所有标准输出
    with redirect_stdout(output_buffer):
        # 在这里调用的任何函数，其print都会被重定向到buffer
        try:
            start_time = time.time()
            result = task_func(item)
        except Exception as e:
            # 错误信息也会被捕获
            print(f"任务{task_id}发生错误: {e}")
            result = None
    
    # 3. 获取所有捕获的输出
    print_output = output_buffer.getvalue()
    
    # 4. 发送到队列（添加超时）
    try:
        output_queue.put((idx, result, print_output, time.time() - start_time), timeout=5)
    except Exception as e:
        print(f"任务{idx} 队列写入超时: {e}", file=sys.stderr)
        # 直接打印到stderr作为备份
        print(f"[备用输出] 任务{idx}: {all_output}", file=sys.stderr)


    # 5. 将结果和输出一起返回（或发送到队列）
    return {
        'task_id': idx,
        'result': result,
        'print_output': print_output
    } 

def my_task_example(x):
    """示例任务函数，包含子函数调用"""
    print(f"[任务开始] 处理数据: {x}")
    time.sleep(x * 0.2)
    
    # 子函数调用
    result_a = sub_func_a(x)
    print(f"子函数A结果: {result_a}")
    
    result_b = sub_func_b(x)
    print(f"子函数B结果: {result_b}")
    
    final_result = result_a + result_b
    print(f"[任务结束] 最终结果: {final_result}")
    
    return final_result

def sub_func_a(x):
    """子函数A"""
    print(f"  [子函数A] 计算 {x} * 2")
    time.sleep(0.05)
    return x * 2

def sub_func_b(x):
    """子函数B"""
    print(f"  [子函数B] 计算 {x} ** 2")
    time.sleep(0.05)
    return x ** 2

def output_collector(output_queue, n_tasks, stop_event):
    """
    独立的输出收集线程
    避免在主进程中阻塞
    """
    collected = {}

    # static variable
    if not hasattr(output_collector,"curr"):
        output_collector.curr=0
     
    while not stop_event.is_set() or not output_queue.empty():
        try:
            # 非阻塞获取，避免死锁
            idx, result, output, elapsed = output_queue.get(timeout=0.5)
            collected[idx] = (result, output, elapsed)
            
            print(f"[收集器] 收到任务{idx}的输出 ({elapsed:.2f}s)")
            
            # 保序输出
            curr = output_collector.curr
            
            while collected.get(curr) != None:
                print(collected[curr][1])
                curr +=1
            output_collector.curr = curr
            
            if curr==n_tasks:
                output_collector.curr=0

        except Empty:
            continue
        except Exception as e:
            print(f"[收集器错误] {e}", file=sys.stderr)
    
    return collected

def safe_parallel_ordered(task_func, data_list, num_workers=None):
    """
    安全的并行顺序处理，避免死锁
    """
    n_tasks = len(data_list)
    num_workers = num_workers or min(4, n_tasks)
    
    print(f"🚀 开始 {n_tasks} 个并行任务")
    print("=" * 60)
    
    # 使用Manager.Queue而不是直接mp.Queue
    manager = mp.Manager()
    output_queue = manager.Queue()
    stop_event = manager.Event()
    
    start_time = time.time()
    
    # 启动输出收集线程
    collector_thread = threading.Thread(
        target=output_collector,
        args=(output_queue, n_tasks, stop_event),
        daemon=True
    )
    collector_thread.start()
    
    try:
        # 使用进程池执行
        with mp.Pool(num_workers) as pool:
            # 准备参数
            args = [(i, item, output_queue) for i, item in enumerate(data_list)]

            # 准备参数: (任务函数, 任务ID, 数据)
            # 注意：这里使用functools.partial固定task_func参数
            from functools import partial
            worker_func = partial(worker_with_output, task_func)

            # 使用imap_unordered避免死锁
            print("开始进程池执行")
            results = pool.starmap_async(worker_func, args)
            
            # 等待所有任务完成（带超时）
            try:
                print("等待所有任务完成（带超时）")
                # it should be longer than the complete time for all task
                task_results = results.get(timeout=3600*10)
                print(f"[主进程] 所有任务执行完成")
                
            except mp.TimeoutError:
                print("⚠️  任务执行超时，强制终止进程池")
                pool.terminate()
                pool.join()
                raise TimeoutError("任务执行超时")
        
        # 通知收集器停止
        stop_event.set()
        collector_thread.join(timeout=2)
        print(f"[主进程] 收集器停止")

    except Exception as e:
        print(f"❌ 并行执行出错: {e}")
        stop_event.set()
        raise
    
    elapsed = time.time() - start_time
    print(f"\n⏱️  所有任务提交完成，耗时: {elapsed:.2f}s")
    
    # 注意：这里我们无法从collector_thread获取结果
    # 我们需要另一种方式收集结果
    
    return [x["result"] for x in task_results]

# 使用示例
if __name__ == "__main__":
    data = range(32)
    try:
        results = safe_parallel_ordered(my_task_example,data, num_workers=4)
        print(f"结果: {results}")
    except Exception as e:
        print(f"执行失败: {e}")

    data = range(32,64)
    try:
        results = safe_parallel_ordered(my_task_example,data, num_workers=4)
        print(f"结果: {results}")
    except Exception as e:
        print(f"执行失败: {e}")

