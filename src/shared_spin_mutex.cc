#include "spaghetti/include/shared_spin_mutex.h"

std::unique_ptr<SharedSpinMutex> new_mutex()
{
    return std::unique_ptr<SharedSpinMutex>(new SharedSpinMutex());
}