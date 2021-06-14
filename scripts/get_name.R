get_name <- function(population,
                     hotspot_percent,
                     hotspot_size) {
  hotspot = sample(1:population, 1) %% 100
  
  if (hotspot <= hotspot_percent) {
    cust_id = (sample(1:population, 1) %% hotspot_size) + 1
  } else {
    cust_id = sample(1:population, 1)
  }
  
  return(cust_id)
  
}

get_name2 <- function(accounts) {
  if (accounts == 10) {
    cust_id = sample(1:accounts, 1)
  } else if( accounts == 100) {
    cust_id = sample(1:accounts, 1)
  } else {
    n = runif(1)
    if (n < 0.25) {
      cust_id = sample(1:100, 1)
    } else {
      cust_id = sample(1:accounts, 1)
    }
  }
  return(cust_id)
}

n = 10000
pop = 10000
d = rep(0, n)
w = rep(0, n)

for (i in 1:n) {
  d[i] = get_name(pop, 25, 100)
  w[i] = get_name2(pop)
  
}

hist(d)
hist(w)




