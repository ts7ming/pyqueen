import cProfile
import pstats
from memory_profiler import profile as watch_mem

def watch_func(func):
    def profiled_func(*args, **kwargs):
        profile = cProfile.Profile()
        profile.enable()
        result = func(*args, **kwargs)
        profile.disable()
        print('==================================  分析结果 Begin ==================================')
        pstats.Stats(profile).strip_dirs().sort_stats('cumtime').print_stats(10)
        print('==================================  分析结果  End ==================================')
        return result

    return profiled_func

def watch_line(func):
    def profiled_func(*args, **kwargs):
        import line_profiler
        lp = line_profiler.LineProfiler()
        lp_wrapper = lp(func)
        result = lp_wrapper(*args, **kwargs)
        print('==================================  分析结果 Begin ==================================')
        lp.print_stats()
        print('==================================  分析结果  End ==================================')
        return result

    return profiled_func


