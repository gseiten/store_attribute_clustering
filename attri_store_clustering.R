#getting the apparels dumped in 
cat("\n\n")
cat("Cleaning enviornment","\n")
rm(list=ls()) 
gc()
ls_results1<-list()
#---- Sourcing Libraries and external files ----
# loading libraries
cat("\n\n")
cat("Loading libraries","\n")

library(dplyr)
library(data.table)
library(pbapply)
library(arrow)
library(cluster)
library(tidyr)
library(stringr)
library(NbClust)
library(factoextra)
library(data.table)

# sourcing external file
cat("\n\n")
cat("Sourcing external files", "\n")

#defining the path of the external files
path_proj<-"D:/products/attribute_store_clustering"
#path_dbase<-"D:/database/txn_extra_attr/01JUL2021 to 31DEC2021"
path_dbase<-"D:/database/txn_extra_attr/input_apparel"
path_lib <- file.path("D:/store_analytics/auto/libraries")
path_dept_mstr<-"D:/database/txn/offline/generate/day1/01APR2021-01APR2021_BILLS.parquet"
lis_files<-list.files(path_dbase)

source(file.path(path_lib, "read_folder_function(READ_FOLDER).R"))

file_path_attr<-file.path(path_proj,"input")
files<-list.files(file_path_attr)
dt_attr<-fread(file.path(file_path_attr,files))


#get the unique departments
unique_departments<-sort(unique(dt_attr$department))

#reading all the files present in a folder
ls_txn <- READ_FOLDER(path_dbase, contain = paste0(unique_departments, collapse = "|"), patternFile = "parquet", 
                      selCols = c("STORE","ARTICLENAME",
                                  "MRPAMT",
                                  "DESC3","CNAME2","CNAME3","CNAME6","UDFSTRING01","UDFSTRING02","UDFSTRING03","UDFSTRING04","UDFSTRING05","UDFSTRING06",
                                  "UDFSTRING07","UDFSTRING08","UDFSTRING09","UDFSTRING10","QTY","DISCOUNTAMT" ),
                      outObj="list"
                      )
dt_txn<-ls_txn[[1]]
names_txn<-ls_txn[[2]]
names(dt_txn)<-names_txn
#final list is in dt_txn
dt_txn<-rbindlist(dt_txn,idcol = "DEPARTMENT")
dt_txn$DEPARTMENT<-data.table(str_split_fixed(dt_txn$DEPARTMENT, ".parquet", 2))$V1
#include only specific columns
dt_txn=dt_txn[,c("DEPARTMENT","STORE","DESC3","CNAME2","CNAME3","CNAME6","UDFSTRING01","UDFSTRING02",
                "UDFSTRING03","UDFSTRING04","UDFSTRING05","UDFSTRING06",
                "UDFSTRING07","UDFSTRING08","UDFSTRING09","UDFSTRING10","QTY","DISCOUNTAMT")]
unique_departments<-unique(dt_txn$DEPARTMENT)
#cleaning the data 

dt_txn<-dt_txn[(dt_txn$DISCOUNTAMT==0 & dt_txn$QTY>0),]
dt_txn<-dt_txn[!grep("FO-",dt_txn$STORE)]
dt_txn<-dt_txn[!grep("NFO-",dt_txn$STORE)]
dt_txn<-dt_txn[!grep("-WH",dt_txn$STORE)]

cbindlist <- function(list) {
  n <- length(list)
  res <- NULL
  for (i in seq(n)) res <- cbind(res, list[[i]])
  return(res)
}


dt_txn<-dt_txn[order(STORE)]
ls_final_result<-list()
ls_fitness<-list()
dept_ind<-1

#get the important attributes corresponding to each department
for(dept in unique_departments)
{
  
  
  imp_attr<-dt_attr[which(dt_attr$department==dept & dt_attr$selected==1),]$attribute
  dt<-dt_txn[which(dt_txn$DEPARTMENT==dept),]
  dt_store<-dt[,c("STORE","DEPARTMENT","QTY")]
  dt_store_join<-dt[,c("STORE")]
  dt_store_join<-dt_store_join[which(!duplicated(dt_store_join$STORE)),]
  ls_results<-list()
  
  cat("mapping with the attribute")
  for(attr_ind in 1:length(imp_attr))
    {
   
    dt_imp_attr<-dt[,(grep(imp_attr[attr_ind],colnames(dt))):(grep(imp_attr[attr_ind],colnames(dt))+1)]
    dt_imp_attr<-dt_imp_attr[,1]
    dt_store1<-dt_store
    dt_store1$imp_attr<-dt_imp_attr
    #performing the manipulations
    dt_store1<-dt_store1[order(STORE),.( total_qty = sum(QTY, na.rm = TRUE)),.(STORE,imp_attr)]
    dt_store1<-dt_store1[(dt_store1$imp_attr!=""),]%>% pivot_wider(names_from = "imp_attr", values_from ="total_qty")
    dt_store1[is.na(dt_store1)]<-0
    dt_store1$total_sales<-rowSums(dt_store1[,2:ncol(dt_store1)])
    dt_store1[,2:(ncol(dt_store1)-1)]<-dt_store1[,2:(ncol(dt_store1)-1)]/dt_store1$total_sales
    dt_store1<-left_join(dt_store_join,dt_store1,by="STORE")
    dt_store1[is.na(dt_store1)]<-0
    dt_store1<-dt_store1[ ,-c("total_sales")]
    ls_results[[attr_ind]]<-dt_store1
    
  }

  
  #dt_dept<-cbindlist(ls_results)
  #dt_dept2<-dt_dept[,!duplicated(colnames(dt_dept))]
  dt_dept<-ls_results[[1]]
  if(length(ls_results)>1)
  {
  for(i in 2:length(ls_results))
  {
    dt_dept<-left_join(dt_dept,ls_results[[i]],by="STORE")
  }
  }
  
  
  #applying the elbow method for K-Means
  #set.seed(123)
  # Compute and plot wss for k = 2 to k = 15.
  
  
  k.min<-25
  k.max <-35
  data <- dt_dept[,2:(ncol(dt_dept))]
  wss <- sapply(k.min:k.max, 
                function(k){kmeans(data, k, nstart=50,iter.max = 100 )$tot.withinss})
  wss
  ls_slopes<-list()
  for(w in 1:(length(wss)-1))
  {
    ls_slopes[w]<-floor(wss[w+1]-wss[w])
    
  }
  
  
  #function to calculate average silhoutee method
  cat("getting the optimal value of k")
  avg_sil <- function(k,dt_dept) {
    km.res <- kmeans(dt_dept, centers = k,iter.max=10,nstart = 25)
    #print(km.res)
    ss <- silhouette(km.res$cluster, dist(dt_dept))
    return(mean(ss[, 3]))
  }

  k.values <- 25:35

  
  # extract avg silhouette for 2-15 clusters
  #avg_sil_values <- map_dbl(k.values, avg_sil)
  avg_sil_values<-list()
  index<-1
  for(k in k.values)
  {
    avg_sil_values[[index]]<-avg_sil(k,data)
    index<-index+1

  }
  df_sil<-data.table("cluster"  = k.values, "avg" = avg_sil_values) 
  k<-df_sil[which.max(df_sil$avg),]$cluster
  # plot(k.values, avg_sil_values,
  #      type = "b", pch = 10, frame = FALSE,
  #      xlab = "Number of clusters K",
  #      ylab = "Average Silhouettes")
  
  mdl<-kmeans(data,centers=k,iter.max =10)
  fitness<-(mdl$betweenss/mdl$totss)
  ls_fitness[[dept_ind]]<-data.table("department"=dept,"goodness_of_fit"=fitness)

  
  #ss <- silhouette(mdl$cluster, dist(dt_dept))
  cat("getting the subattri_contribution")
  #writing the% contribution of each sub-attribute in attribute in a particular folder 
  file_out_contri_path<-file.path(path_proj, "output_sub_attri",paste0(dept,"_subattri_contribution.csv"))
  dt_dept$clstr_number<-mdl$cluster
  fwrite(dt_dept,file_out_contri_path)
  
  #calculating the %contribution of each attribute for coresspondng clusters
  cat("getting the subattri_avg_contri")
  data$clstr_number<-mdl$cluster
  data$count<-1
  setDT(data)
  df_count<-data[,(lapply(.SD, sum, na.rm = TRUE)),.(clstr_number)]
  df_grped<-data[,(lapply(.SD, mean, na.rm = TRUE)),.(clstr_number)]
  df_grped[,2:(ncol(df_grped)-1)]<-df_grped[,2:(ncol(df_grped)-1)]
  #df_grped[,2:(ncol(df_grped)-1)]<-round(df_grped[,2:(ncol(df_grped)-1)],3)
  df_grped$store_count<-df_count$count
  df_grped = subset(df_grped, select = -c(count) )
  df_grped<-df_grped[order(clstr_number)]
  #writing the %contribution 
  file_out <- file.path(path_proj, paste0(dept,"_subattr_avg_%contribution.csv"))
  fwrite(df_grped,file_out)
  
  
  
  #data frame having the store and the corresponding cluster
  df<-data.frame("STORE"=dt_store_join$STORE,
                 dept=mdl$cluster)
  colnames(df)<-c("STORE", paste0(dept,"_clstr"))
  ls_final_result[[dept_ind]]<-df
  
  dept_ind<-dept_ind+1
  
  
  
  print(dept_ind)
  
  
  
}  
  

for(i in 1:length(ls_final_result))
{
  print(nrow(ls_final_result[[i]]))
}

df_final<-ls_final_result[[1]]
for(i in 2:length(ls_final_result))
{
  df_final<-full_join(df_final,ls_final_result[[i]],by="STORE")
}

file_out <- file.path(path_proj, "output", "1JUL_31_DEC_DEPT_CLSTR.csv")

fwrite(df_final, file.path(file_out))



ls_fitness<-rbindlist(ls_fitness)
file_out_dept <- file.path(path_proj, "output", "dept_goodness_fit.csv")
fwrite(ls_fitness, file.path(file_out_dept))








# for(i in 1:length(ls_results))
# {
#   print(length(ls_results[[i]]))
# }
# 
# 
# df_jeans<-read_parquet("D:/database/txn_extra_attr/dept1/Jeans[L].parquet")

#df<-rbindlist(lapply(ls_txn,fread), idcol = "Department")
#dt_dept<-read_parquet(path_dept_mstr)
#dt_txn<-left_join(dt_txn,dt_dept,by=c("ARTICLENAME"="Article_Name"))
