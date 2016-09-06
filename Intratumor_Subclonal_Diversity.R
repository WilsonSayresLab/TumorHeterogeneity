#Intratumor_Subclonal_Diversity

#This scripts assesses the extent of intratumor subclonal variation in the simulated tumors across all parameter values 

#rm(list = ls())

tumors.all.AA9 = read.table("tumors_detectable_final_live_AA9.csv",header=T,sep=",")
tumors.all.AB9 = read.table("tumors_detectable_final_live_AB9.csv",header=T,sep=",")
tumors.all.AC9 = read.table("tumors_detectable_final_live_AC9.csv",header=T,sep=",")
tumors.all.AD9 = read.table("tumors_detectable_final_live_AD9.csv",header=T,sep=",")
tumors.all.BA9 = read.table("tumors_detectable_final_live_BA9.csv",header=T,sep=",")
tumors.all.BB9 = read.table("tumors_detectable_final_live_BB9.csv",header=T,sep=",")
tumors.all.BC9 = read.table("tumors_detectable_final_live_BC9.csv",header=T,sep=",")
tumors.all.BD9 = read.table("tumors_detectable_final_live_BD9.csv",header=T,sep=",")
tumors.all.CA9 = read.table("tumors_detectable_final_live_CA9.csv",header=T,sep=",")
tumors.all.CB9 = read.table("tumors_detectable_final_live_CB9.csv",header=T,sep=",")
tumors.all.CC9 = read.table("tumors_detectable_final_live_CC9.csv",header=T,sep=",")
tumors.all.CD9 = read.table("tumors_detectable_final_live_CD9.csv",header=T,sep=",")

tumors.ids.AA9 = read.table("detectable_tumorids_AA9.csv",header=T,sep=",")
tumors.ids.AB9 = read.table("detectable_tumorids_AB9.csv",header=T,sep=",")
tumors.ids.AC9 = read.table("detectable_tumorids_AC9.csv",header=T,sep=",")
tumors.ids.AD9 = read.table("detectable_tumorids_AD9.csv",header=T,sep=",")
tumors.ids.BA9 = read.table("detectable_tumorids_BA9.csv",header=T,sep=",")
tumors.ids.BB9 = read.table("detectable_tumorids_BB9.csv",header=T,sep=",")
tumors.ids.BC9 = read.table("detectable_tumorids_BC9.csv",header=T,sep=",")
tumors.ids.BD9 = read.table("detectable_tumorids_BD9.csv",header=T,sep=",")
tumors.ids.CA9 = read.table("detectable_tumorids_CA9.csv",header=T,sep=",")
tumors.ids.CB9 = read.table("detectable_tumorids_CB9.csv",header=T,sep=",")
tumors.ids.CC9 = read.table("detectable_tumorids_CC9.csv",header=T,sep=",")
tumors.ids.CD9 = read.table("detectable_tumorids_CD9.csv",header=T,sep=",")

min.size.subclones=100

####AA9
subclones.g10frequency.vector.AA9=vector()
subclones.l10frequency.vector.AA9=vector()

subclones.g10frequencies.AA9=vector()

for (i in 1:nrow(tumors.ids.AA9)){
  
  #tumors.AA9 = subset(tumors.all.AA9, (tumors.all.AA9$tumorid == tumors.ids.AA9[i,]) & (tumors.all.AA9$finalsnapshot == 'Y') & (tumors.all.AA9$cells>=200)) 
  
  tumors.AA9 = subset(tumors.all.AA9, (tumors.all.AA9$tumorid == tumors.ids.AA9[i,]) & (tumors.all.AA9$cells>=min.size.subclones))
  
  tumors.AA9$frequency=(tumors.AA9$cells/sum(as.numeric(tumors.AA9$cells)))*100
  
  subclones.g10frequency.AA9=length(which(tumors.AA9$frequency>=10))
  
  subclones.l10frequency.AA9=length(which(tumors.AA9$frequency<10))
  
  subclones.g10frequency.vector.AA9=c(subclones.g10frequency.vector.AA9, subclones.g10frequency.AA9)
  subclones.l10frequency.vector.AA9=c(subclones.l10frequency.vector.AA9, subclones.l10frequency.AA9)
  
  subclones.g10frequencies.AA9=c(subclones.g10frequencies.AA9,sum(tumors.AA9$frequency[which(tumors.AA9$frequency>=10)]))
  
}

avg.subclones.g10frequency.AA9=mean(subclones.g10frequency.vector.AA9)
avg.subclones.l10frequency.AA9=mean(subclones.l10frequency.vector.AA9)

avg.subclones.g10frequencies.AA9=mean(subclones.g10frequencies.AA9)
 
#sd.subclones.g10frequency.AA9=sd(subclones.g10frequency.vector.AA9)*1.96/sqrt(length(subclones.g10frequency.vector.AA9))
#sd.subclones.l10frequency.AA9=sd(subclones.l10frequency.vector.AA9)*1.96/sqrt(length(subclones.l10frequency.vector.AA9))


####AB9
subclones.g10frequency.vector.AB9=vector()
subclones.l10frequency.vector.AB9=vector()

subclones.g10frequencies.AB9=vector()

for (i in 1:nrow(tumors.ids.AB9)) {
  
  tumors.AB9 = subset(tumors.all.AB9, (tumors.all.AB9$tumorid == tumors.ids.AB9[i,]) & (tumors.all.AB9$cells>=min.size.subclones))
  
  tumors.AB9$frequency=(tumors.AB9$cells/sum(as.numeric(tumors.AB9$cells)))*100
  
  subclones.g10frequency.AB9=length(which(tumors.AB9$frequency>=10))
  
  subclones.l10frequency.AB9=length(which(tumors.AB9$frequency<10))
  
  subclones.g10frequency.vector.AB9=c(subclones.g10frequency.vector.AB9, subclones.g10frequency.AB9)
  subclones.l10frequency.vector.AB9=c(subclones.l10frequency.vector.AB9, subclones.l10frequency.AB9)
  
  subclones.g10frequencies.AB9=c(subclones.g10frequencies.AB9,sum(tumors.AB9$frequency[which(tumors.AB9$frequency>=10)]))
  
}

avg.subclones.g10frequency.AB9=mean(subclones.g10frequency.vector.AB9)
avg.subclones.l10frequency.AB9=mean(subclones.l10frequency.vector.AB9)

avg.subclones.g10frequencies.AB9=mean(subclones.g10frequencies.AB9)

#sd.subclones.g10frequency.AB9=sd(subclones.g10frequency.vector.AB9)*1.96/sqrt(length(subclones.g10frequency.vector.AB9))
#sd.subclones.l10frequency.AB9=sd(subclones.l10frequency.vector.AB9)*1.96/sqrt(length(subclones.l10frequency.vector.AB9))

####AC9
subclones.g10frequency.vector.AC9=vector()
subclones.l10frequency.vector.AC9=vector()

subclones.g10frequencies.AC9=vector()

for (i in 1:nrow(tumors.ids.AC9)) {
  
  tumors.AC9 = subset(tumors.all.AC9, (tumors.all.AC9$tumorid == tumors.ids.AC9[i,]) & (tumors.all.AC9$cells>=min.size.subclones))
  
  tumors.AC9$frequency=(tumors.AC9$cells/sum(as.numeric(tumors.AC9$cells)))*100
  
  subclones.g10frequency.AC9=length(which(tumors.AC9$frequency>=10))
  
  subclones.l10frequency.AC9=length(which(tumors.AC9$frequency<10))
  
  subclones.g10frequency.vector.AC9=c(subclones.g10frequency.vector.AC9, subclones.g10frequency.AC9)
  subclones.l10frequency.vector.AC9=c(subclones.l10frequency.vector.AC9, subclones.l10frequency.AC9)
  
  subclones.g10frequencies.AC9=c(subclones.g10frequencies.AC9,sum(tumors.AC9$frequency[which(tumors.AC9$frequency>=10)]))
  
}

avg.subclones.g10frequency.AC9=mean(subclones.g10frequency.vector.AC9)
avg.subclones.l10frequency.AC9=mean(subclones.l10frequency.vector.AC9)

avg.subclones.g10frequencies.AC9=mean(subclones.g10frequencies.AC9)

#sd.subclones.g10frequency.AC9=sd(subclones.g10frequency.vector.AC9)*1.96/sqrt(length(subclones.g10frequency.vector.AC9))
#sd.subclones.l10frequency.AC9=sd(subclones.l10frequency.vector.AC9)*1.96/sqrt(length(subclones.l10frequency.vector.AC9))


####AD9
subclones.g10frequency.vector.AD9=vector()
subclones.l10frequency.vector.AD9=vector()

subclones.g10frequencies.AD9=vector()

for (i in 1:nrow(tumors.ids.AD9)){
  
  tumors.AD9 = subset(tumors.all.AD9, (tumors.all.AD9$tumorid == tumors.ids.AD9[i,]) & (tumors.all.AD9$cells>=min.size.subclones))
  
  tumors.AD9$frequency=(tumors.AD9$cells/sum(as.numeric(tumors.AD9$cells)))*100
  
  subclones.g10frequency.AD9=length(which(tumors.AD9$frequency>=10))
  
  subclones.l10frequency.AD9=length(which(tumors.AD9$frequency<10))
  
  subclones.g10frequency.vector.AD9=c(subclones.g10frequency.vector.AD9, subclones.g10frequency.AD9)
  subclones.l10frequency.vector.AD9=c(subclones.l10frequency.vector.AD9, subclones.l10frequency.AD9)
  
  subclones.g10frequencies.AD9=c(subclones.g10frequencies.AD9,sum(tumors.AD9$frequency[which(tumors.AD9$frequency>=10)]))

}

avg.subclones.g10frequency.AD9=mean(subclones.g10frequency.vector.AD9)
avg.subclones.l10frequency.AD9=mean(subclones.l10frequency.vector.AD9)

avg.subclones.g10frequencies.AD9=mean(subclones.g10frequencies.AD9)

#sd.subclones.g10frequency.AD9=sd(subclones.g10frequency.vector.AD9)*1.96/sqrt(length(subclones.g10frequency.vector.AD9))
#sd.subclones.l10frequency.AD9=sd(subclones.l10frequency.vector.AD9)*1.96/sqrt(length(subclones.l10frequency.vector.AD9))


####BA9
subclones.g10frequency.vector.BA9=vector()
subclones.l10frequency.vector.BA9=vector()

subclones.g10frequencies.BA9=vector()

for (i in 1:nrow(tumors.ids.BA9)){
  
  tumors.BA9 = subset(tumors.all.BA9, (tumors.all.BA9$tumorid == tumors.ids.BA9[i,]) & (tumors.all.BA9$cells>=min.size.subclones))
  
  tumors.BA9$frequency=(tumors.BA9$cells/sum(as.numeric(tumors.BA9$cells)))*100
  
  subclones.g10frequency.BA9=length(which(tumors.BA9$frequency>=10))
  
  subclones.l10frequency.BA9=length(which(tumors.BA9$frequency<10))
  
  subclones.g10frequency.vector.BA9=c(subclones.g10frequency.vector.BA9, subclones.g10frequency.BA9)
  subclones.l10frequency.vector.BA9=c(subclones.l10frequency.vector.BA9, subclones.l10frequency.BA9)
  
  subclones.g10frequencies.BA9=c(subclones.g10frequencies.BA9,sum(tumors.BA9$frequency[which(tumors.BA9$frequency>=10)]))
  
}

avg.subclones.g10frequency.BA9=mean(subclones.g10frequency.vector.BA9)
avg.subclones.l10frequency.BA9=mean(subclones.l10frequency.vector.BA9)

avg.subclones.g10frequencies.BA9=mean(subclones.g10frequencies.BA9)

#sd.subclones.g10frequency.BA9=sd(subclones.g10frequency.vector.BA9)*1.96/sqrt(length(subclones.g10frequency.vector.BA9))
#sd.subclones.l10frequency.BA9=sd(subclones.l10frequency.vector.BA9)*1.96/sqrt(length(subclones.l10frequency.vector.BA9))


####BB9
subclones.g10frequency.vector.BB9=vector()
subclones.l10frequency.vector.BB9=vector()

subclones.g10frequencies.BB9=vector()

for (i in 1:nrow(tumors.ids.BB9)) {
  
  tumors.BB9 = subset(tumors.all.BB9, (tumors.all.BB9$tumorid == tumors.ids.BB9[i,]) & (tumors.all.BB9$cells>=min.size.subclones))
  
  tumors.BB9$frequency=(tumors.BB9$cells/sum(as.numeric(tumors.BB9$cells)))*100
  
  subclones.g10frequency.BB9=length(which(tumors.BB9$frequency>=10))
  
  subclones.l10frequency.BB9=length(which(tumors.BB9$frequency<10))
  
  subclones.g10frequency.vector.BB9=c(subclones.g10frequency.vector.BB9, subclones.g10frequency.BB9)
  subclones.l10frequency.vector.BB9=c(subclones.l10frequency.vector.BB9, subclones.l10frequency.BB9)
  
  subclones.g10frequencies.BB9=c(subclones.g10frequencies.BB9,sum(tumors.BB9$frequency[which(tumors.BB9$frequency>=10)]))
  
}

avg.subclones.g10frequency.BB9=mean(subclones.g10frequency.vector.BB9)
avg.subclones.l10frequency.BB9=mean(subclones.l10frequency.vector.BB9)

avg.subclones.g10frequencies.BB9=mean(subclones.g10frequencies.BB9)

#sd.subclones.g10frequency.BB9=sd(subclones.g10frequency.vector.BB9)*1.96/sqrt(length(subclones.g10frequency.vector.BB9))
#sd.subclones.l10frequency.BB9=sd(subclones.l10frequency.vector.BB9)*1.96/sqrt(length(subclones.l10frequency.vector.BB9))


####BC9
subclones.g10frequency.vector.BC9=vector()
subclones.l10frequency.vector.BC9=vector()

subclones.g10frequencies.BC9=vector()

for (i in 1:nrow(tumors.ids.BC9)){
  
  tumors.BC9 = subset(tumors.all.BC9, (tumors.all.BC9$tumorid == tumors.ids.BC9[i,]) & (tumors.all.BC9$cells>=min.size.subclones))
  
  tumors.BC9$frequency=(tumors.BC9$cells/sum(as.numeric(tumors.BC9$cells)))*100
  
  subclones.g10frequency.BC9=length(which(tumors.BC9$frequency>=10))
  
  subclones.l10frequency.BC9=length(which(tumors.BC9$frequency<10))
  
  subclones.g10frequency.vector.BC9=c(subclones.g10frequency.vector.BC9, subclones.g10frequency.BC9)
  subclones.l10frequency.vector.BC9=c(subclones.l10frequency.vector.BC9, subclones.l10frequency.BC9)
  
  subclones.g10frequencies.BC9=c(subclones.g10frequencies.BC9,sum(tumors.BC9$frequency[which(tumors.BC9$frequency>=10)]))
  
}

avg.subclones.g10frequency.BC9=mean(subclones.g10frequency.vector.BC9)
avg.subclones.l10frequency.BC9=mean(subclones.l10frequency.vector.BC9)

avg.subclones.g10frequencies.BC9=mean(subclones.g10frequencies.BC9)

#sd.subclones.g10frequency.BC9=sd(subclones.g10frequency.vector.BC9)*1.96/sqrt(length(subclones.g10frequency.vector.BC9))
#sd.subclones.l10frequency.BC9=sd(subclones.l10frequency.vector.BC9)*1.96/sqrt(length(subclones.l10frequency.vector.BC9))


####BD9
subclones.g10frequency.vector.BD9=vector()
subclones.l10frequency.vector.BD9=vector()

subclones.g10frequencies.BD9=vector()

for (i in 1:nrow(tumors.ids.BD9)){
  
  tumors.BD9 = subset(tumors.all.BD9, (tumors.all.BD9$tumorid == tumors.ids.BD9[i,]) & (tumors.all.BD9$cells>=min.size.subclones))
  
  tumors.BD9$frequency=(tumors.BD9$cells/sum(as.numeric(tumors.BD9$cells)))*100
  
  subclones.g10frequency.BD9=length(which(tumors.BD9$frequency>=10))
  
  subclones.l10frequency.BD9=length(which(tumors.BD9$frequency<10))
  
  subclones.g10frequency.vector.BD9=c(subclones.g10frequency.vector.BD9, subclones.g10frequency.BD9)
  subclones.l10frequency.vector.BD9=c(subclones.l10frequency.vector.BD9, subclones.l10frequency.BD9)
  
  subclones.g10frequencies.BD9=c(subclones.g10frequencies.BD9,sum(tumors.BD9$frequency[which(tumors.BD9$frequency>=10)]))
  
}

avg.subclones.g10frequency.BD9=mean(subclones.g10frequency.vector.BD9)
avg.subclones.l10frequency.BD9=mean(subclones.l10frequency.vector.BD9)

avg.subclones.g10frequencies.BD9=mean(subclones.g10frequencies.BD9)

#sd.subclones.g10frequency.BD9=sd(subclones.g10frequency.vector.BD9)*1.96/sqrt(length(subclones.g10frequency.vector.BD9))
#sd.subclones.l10frequency.BD9=sd(subclones.l10frequency.vector.BD9)*1.96/sqrt(length(subclones.l10frequency.vector.BD9))

####CA9
subclones.g10frequency.vector.CA9=vector()
subclones.l10frequency.vector.CA9=vector()

subclones.g10frequencies.CA9=vector()

for (i in 1:nrow(tumors.ids.CA9)){
  
  tumors.CA9 = subset(tumors.all.CA9, (tumors.all.CA9$tumorid == tumors.ids.CA9[i,]) & (tumors.all.CA9$cells>=min.size.subclones))
  
  tumors.CA9$frequency=(tumors.CA9$cells/sum(as.numeric(tumors.CA9$cells)))*100
  
  subclones.g10frequency.CA9=length(which(tumors.CA9$frequency>=10))
  
  subclones.l10frequency.CA9=length(which(tumors.CA9$frequency<10))
  
  subclones.g10frequency.vector.CA9=c(subclones.g10frequency.vector.CA9, subclones.g10frequency.CA9)
  subclones.l10frequency.vector.CA9=c(subclones.l10frequency.vector.CA9, subclones.l10frequency.CA9)
  
  subclones.g10frequencies.CA9=c(subclones.g10frequencies.CA9,sum(tumors.CA9$frequency[which(tumors.CA9$frequency>=10)]))
  
}

avg.subclones.g10frequency.CA9=mean(subclones.g10frequency.vector.CA9)
avg.subclones.l10frequency.CA9=mean(subclones.l10frequency.vector.CA9)

avg.subclones.g10frequencies.CA9=mean(subclones.g10frequencies.CA9)

#sd.subclones.g10frequency.CA9=sd(subclones.g10frequency.vector.CA9)*1.96/sqrt(length(subclones.g10frequency.vector.CA9))
#sd.subclones.l10frequency.CA9=sd(subclones.l10frequency.vector.CA9)*1.96/sqrt(length(subclones.l10frequency.vector.CA9))


####CB9
subclones.g10frequency.vector.CB9=vector()
subclones.l10frequency.vector.CB9=vector()

subclones.g10frequencies.CB9=vector()

for (i in 1:nrow(tumors.ids.CB9)){
  
  tumors.CB9 = subset(tumors.all.CB9, (tumors.all.CB9$tumorid == tumors.ids.CB9[i,]) & (tumors.all.CB9$cells>=min.size.subclones))
  
  tumors.CB9$frequency=(tumors.CB9$cells/sum(as.numeric(tumors.CB9$cells)))*100
  
  subclones.g10frequency.CB9=length(which(tumors.CB9$frequency>=10))
  
  subclones.l10frequency.CB9=length(which(tumors.CB9$frequency<10))
  
  subclones.g10frequency.vector.CB9=c(subclones.g10frequency.vector.CB9, subclones.g10frequency.CB9)
  subclones.l10frequency.vector.CB9=c(subclones.l10frequency.vector.CB9, subclones.l10frequency.CB9)
  
  subclones.g10frequencies.CB9=c(subclones.g10frequencies.CB9,sum(tumors.CB9$frequency[which(tumors.CB9$frequency>=10)]))
  
}

avg.subclones.g10frequency.CB9=mean(subclones.g10frequency.vector.CB9)
avg.subclones.l10frequency.CB9=mean(subclones.l10frequency.vector.CB9)

avg.subclones.g10frequencies.CB9=mean(subclones.g10frequencies.CB9)

#sd.subclones.g10frequency.CB9=sd(subclones.g10frequency.vector.CB9)*1.96/sqrt(length(subclones.g10frequency.vector.CB9))
#sd.subclones.l10frequency.CB9=sd(subclones.l10frequency.vector.CB9)*1.96/sqrt(length(subclones.l10frequency.vector.CB9))


####CC9
subclones.g10frequency.vector.CC9=vector()
subclones.l10frequency.vector.CC9=vector()

subclones.g10frequencies.CC9=vector()

for (i in 1:nrow(tumors.ids.CC9)){
  
  tumors.CC9 = subset(tumors.all.CC9, (tumors.all.CC9$tumorid == tumors.ids.CC9[i,]) & (tumors.all.CC9$cells>=min.size.subclones))
  
  tumors.CC9$frequency=(tumors.CC9$cells/sum(as.numeric(tumors.CC9$cells)))*100
  
  subclones.g10frequency.CC9=length(which(tumors.CC9$frequency>=10))
  
  subclones.l10frequency.CC9=length(which(tumors.CC9$frequency<10))
  
  subclones.g10frequency.vector.CC9=c(subclones.g10frequency.vector.CC9, subclones.g10frequency.CC9)
  subclones.l10frequency.vector.CC9=c(subclones.l10frequency.vector.CC9, subclones.l10frequency.CC9)
  
  subclones.g10frequencies.CC9=c(subclones.g10frequencies.CC9,sum(tumors.CC9$frequency[which(tumors.CC9$frequency>=10)]))

}

avg.subclones.g10frequency.CC9=mean(subclones.g10frequency.vector.CC9)
avg.subclones.l10frequency.CC9=mean(subclones.l10frequency.vector.CC9)

avg.subclones.g10frequencies.CC9=mean(subclones.g10frequencies.CC9)

#sd.subclones.g10frequency.CC9=sd(subclones.g10frequency.vector.CC9)*1.96/sqrt(length(subclones.g10frequency.vector.CC9))
#sd.subclones.l10frequency.CC9=sd(subclones.l10frequency.vector.CC9)*1.96/sqrt(length(subclones.l10frequency.vector.CC9))


####CD9
subclones.g10frequency.vector.CD9=vector()
subclones.l10frequency.vector.CD9=vector()

subclones.g10frequencies.CD9=vector()

for (i in 1:nrow(tumors.ids.CD9)){
  
  tumors.CD9 = subset(tumors.all.CD9, (tumors.all.CD9$tumorid == tumors.ids.CD9[i,]) & (tumors.all.CD9$cells>=min.size.subclones))
  
  tumors.CD9$frequency=(tumors.CD9$cells/sum(as.numeric(tumors.CD9$cells)))*100
  
  subclones.g10frequency.CD9=length(which(tumors.CD9$frequency>=10))
  
  subclones.l10frequency.CD9=length(which(tumors.CD9$frequency<10))
  
  subclones.g10frequency.vector.CD9=c(subclones.g10frequency.vector.CD9, subclones.g10frequency.CD9)
  subclones.l10frequency.vector.CD9=c(subclones.l10frequency.vector.CD9, subclones.l10frequency.CD9)
  
  subclones.g10frequencies.CD9=c(subclones.g10frequencies.CD9,sum(tumors.CD9$frequency[which(tumors.CD9$frequency>=10)]))
  
}

avg.subclones.g10frequency.CD9=mean(subclones.g10frequency.vector.CD9)
avg.subclones.l10frequency.CD9=mean(subclones.l10frequency.vector.CD9)

avg.subclones.g10frequencies.CD9=mean(subclones.g10frequencies.CD9)

#sd.subclones.g10frequency.CD9=sd(subclones.g10frequency.vector.CD9)*1.96/sqrt(length(subclones.g10frequency.vector.CD9))
#sd.subclones.l10frequency.CD9=sd(subclones.l10frequency.vector.CD9)*1.96/sqrt(length(subclones.l10frequency.vector.CD9))


avg.subclones.g10frequency.all.tumors=c(avg.subclones.g10frequency.AA9,avg.subclones.g10frequency.AB9,avg.subclones.g10frequency.AC9,avg.subclones.g10frequency.AD9,avg.subclones.g10frequency.BA9,avg.subclones.g10frequency.BB9,avg.subclones.g10frequency.BC9,avg.subclones.g10frequency.BD9,avg.subclones.g10frequency.CA9,avg.subclones.g10frequency.CB9,avg.subclones.g10frequency.CC9,avg.subclones.g10frequency.CD9)
avg.subclones.g10frequency.all.tumors=log10(avg.subclones.g10frequency.all.tumors)
avg.subclones.l10frequency.all.tumors=c(avg.subclones.l10frequency.AA9,avg.subclones.l10frequency.AB9,avg.subclones.l10frequency.AC9,avg.subclones.l10frequency.AD9,avg.subclones.l10frequency.BA9,avg.subclones.l10frequency.BB9,avg.subclones.l10frequency.BC9,avg.subclones.l10frequency.BD9,avg.subclones.l10frequency.CA9,avg.subclones.l10frequency.CB9,avg.subclones.l10frequency.CC9,avg.subclones.l10frequency.CD9)
avg.subclones.l10frequency.all.tumors=log10(avg.subclones.l10frequency.all.tumors)

subclones.g10frequencies=c(avg.subclones.g10frequencies.AA9,avg.subclones.g10frequencies.AB9,avg.subclones.g10frequencies.AC9,avg.subclones.g10frequencies.AD9,avg.subclones.g10frequencies.BA9,avg.subclones.g10frequencies.BB9,avg.subclones.g10frequencies.BC9,avg.subclones.g10frequencies.BD9,avg.subclones.g10frequencies.CA9,avg.subclones.g10frequencies.CB9,avg.subclones.g10frequencies.CC9,avg.subclones.g10frequencies.CD9)

#sd.subclones.g10frequency.all.tumors=c(sd.subclones.g10frequency.AA9,sd.subclones.g10frequency.AB9,sd.subclones.g10frequency.AC9,sd.subclones.g10frequency.AD9,sd.subclones.g10frequency.BA9,sd.subclones.g10frequency.BB9,sd.subclones.g10frequency.BC9,sd.subclones.g10frequency.BD9,sd.subclones.g10frequency.CA9,sd.subclones.g10frequency.CB9,sd.subclones.g10frequency.CC9,sd.subclones.g10frequency.CD9)
#sd.subclones.g10frequency.all.tumors=log10(sd.subclones.g10frequency.all.tumors)
#sd.subclones.l10frequency.all.tumors=c(sd.subclones.l10frequency.AA9,sd.subclones.l10frequency.AB9,sd.subclones.l10frequency.AC9,sd.subclones.l10frequency.AD9,sd.subclones.l10frequency.BA9,sd.subclones.l10frequency.BB9,sd.subclones.l10frequency.BC9,sd.subclones.l10frequency.BD9,sd.subclones.l10frequency.CA9,sd.subclones.l10frequency.CB9,sd.subclones.l10frequency.CC9,sd.subclones.l10frequency.CD9)
#sd.subclones.l10frequency.all.tumors=log10(sd.subclones.l10frequency.all.tumors)

bar_colors=c(rgb(1,0,0,0.6), rgb(0,0,1,0.7))
bar=barplot(rbind(avg.subclones.g10frequency.all.tumors,avg.subclones.l10frequency.all.tumors),beside=FALSE,ylim=c(0,4),col=bar_colors, yaxt="n", main="",names.arg=c("A1","A2","A3","A4","B1","B2","B3","B4","C1","C2","C3","C4"), xlab="", ylab="Average number of subclones", cex.main=1.8, cex.lab=1.5, cex = 1.5, cex.axis=1.6)
#yy=matrix(c(avg.subclones.g10frequency.all.tumors,avg.subclones.l10frequency.all.tumors),2,12,byrow=TRUE)
#ee=matrix(c(sd.subclones.g10frequency.all.tumors,sd.subclones.l10frequency.all.tumors),2,12,byrow=TRUE)
#error.bar(bar,yy,ee)
#arrows(bar,yy+ee, bar, yy, angle=90, code=1, length=0.05)
ticks <- seq(0, 4, by=1)
labels <- sapply(ticks, function(i) as.expression(bquote(10^.(i))))
axis(2, at=c(0,1,2,3,4), labels=labels, cex.axis=1.8)
legend("topleft",legend=c("of size >=10% frequeny","of size <10% frequency"),fill=bar_colors,bty="n",cex=1.5)
#legend("topleft",legend=c("Macroheterogeneity","Microheterogeneity"), fill=bar_colors, bty="n",cex=1.5)