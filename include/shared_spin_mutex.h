#pragma once
#include <memory>
#include <atomic>
#include <cassert>
#include <stdint.h>

struct SharedSpinMutex
{

private:
    std::atomic<uint8_t> lock_;
    uint64_t payload;

public:
    SharedSpinMutex() : lock_(0), payload(0) {}

    void inc_payload()
    {
        payload++;
    }

    uint64_t get_payload() const
    {
        return payload;
    }

    inline void lock()
    {
        bool xchg = false;
        uint8_t l = lock_.load();
        uint8_t s = (1u << 7);
        while (!xchg)
        {
            if (l == 0)
            {

                xchg = lock_.compare_exchange_weak(l, s);
            }
            l = lock_;
        }
    }

    inline void unlock()
    {
        auto l = lock_.load();
        assert(l == (1u << 7));
        auto xchg = false;
        while (!xchg)
        {
            xchg = lock_.compare_exchange_weak(l, 0);
        }
    }

    inline void lock_shared()
    {
        auto xchg = false;
        auto l = lock_.load();
        while (!xchg)
        {
            if (l != (1u << 7) && l < (1u << 7) - 2)
            {
                xchg = lock_.compare_exchange_weak(l, (l + 1));
            }
            l = lock_;
        }
    }

    inline void unlock_shared()
    {
        auto l = lock_.load();
        assert(l != (1u << 7));
        auto xchg = false;
        while (!xchg)
        {
            xchg = lock_.compare_exchange_weak(l, (l - 1));
            l = lock_;
        }
    }
};

std::unique_ptr<SharedSpinMutex> new_mutex();