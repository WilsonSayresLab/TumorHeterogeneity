#rm(list = ls())

#This scripts computes the probability density functions for the sizes of therapeutically resistant subclones within in the primary simulated tumors.

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

old.par <- par(mfrow=c(3, 2))

####AC9

#tumors.all.cells=vector()

tumors.all.resistance.cells=vector()

for (i in 1:nrow(tumors.ids.AC9)){
  
  tumors.AC9 = subset(tumors.all.AC9, (tumors.all.AC9$tumorid == tumors.ids.AC9[i,]) & (tumors.all.AC9$cells>=min.size.subclones) & (tumors.all.AC9$mutations>0))
  
  tumors.AC9.resistance=subset(tumors.AC9,tumors.AC9$mutation_selection<1e-08)
  
  #tumors.all.cells=c(tumors.all.cells,tumors.AD9$cells)
  
  tumors.all.resistance.cells=c(tumors.all.resistance.cells,tumors.AC9.resistance$cells)
  
}

##Density plot
d <- density(log10(tumors.all.resistance.cells))
#d_resistance = density(log10(tumors.all.resistance.cells))
plot(d,xaxt="n",xlim=c(1,10),main="",ylab="Density",xlab="Subclone size (number of cells)",cex.main=1.8,cex.lab=1.5,cex = 1.5,cex.axis=1.6, col=rgb(0,0,1,0.7))
#plot(range(d$x, d$x), range(d$y, d$y), type = "n")
#lines(d,col=rgb(0,0,1,0.7))
#lines(d_resistance,col=rgb(1,0,0,0.6))
polygon(d, col=rgb(0,0,1,0.7))
#polygon(d_resistance, col=rgb(1,0,0,0.6))
ticks <- seq(2, 10, by=2)
labels <- sapply(ticks, function(i) as.expression(bquote(10^.(i))))
axis(1, at=c(2,4,6,8,10), labels=labels, cex.axis=1.6)


####AD9

#tumors.all.cells=vector()

tumors.all.resistance.cells=vector()

for (i in 1:nrow(tumors.ids.AD9)){
  
  tumors.AD9 = subset(tumors.all.AD9, (tumors.all.AD9$tumorid == tumors.ids.AD9[i,]) & (tumors.all.AD9$cells>=min.size.subclones) & (tumors.all.AD9$mutations>0))
  
  tumors.AD9.resistance=subset(tumors.AD9,tumors.AD9$mutation_selection<1e-08)
  
  #tumors.all.cells=c(tumors.all.cells,tumors.AD9$cells)
  
  tumors.all.resistance.cells=c(tumors.all.resistance.cells,tumors.AD9.resistance$cells)
  
}

##Density plot
d <- density(log10(tumors.all.resistance.cells))
#d_resistance = density(log10(tumors.all.resistance.cells))
plot(d,xaxt="n",xlim=c(1,10),main="",ylab="Density",xlab="Subclone size (number of cells)",cex.main=1.8,cex.lab=1.5,cex = 1.5,cex.axis=1.6, col=rgb(0,0,1,0.7))
#plot(range(d$x, d$x), range(d$y, d$y), type = "n")
#lines(d,col=rgb(0,0,1,0.7))
#lines(d_resistance,col=rgb(1,0,0,0.6))
polygon(d, col=rgb(0,0,1,0.7))
#polygon(d_resistance, col=rgb(1,0,0,0.6))
ticks <- seq(2, 10, by=2)
labels <- sapply(ticks, function(i) as.expression(bquote(10^.(i))))
axis(1, at=c(2,4,6,8,10), labels=labels, cex.axis=1.6)


####BC9

#tumors.all.cells=vector()

tumors.all.resistance.cells=vector()

for (i in 1:nrow(tumors.ids.BC9)){
  
  tumors.BC9 = subset(tumors.all.BC9, (tumors.all.BC9$tumorid == tumors.ids.BC9[i,]) & (tumors.all.BC9$cells>=min.size.subclones) & (tumors.all.BC9$mutations>0))
  
  tumors.BC9.resistance=subset(tumors.BC9,tumors.BC9$mutation_selection<1e-08)
  
  #tumors.all.cells=c(tumors.all.cells,tumors.AD9$cells)
  
  tumors.all.resistance.cells=c(tumors.all.resistance.cells,tumors.BC9.resistance$cells)
  
}

##Density plot
d <- density(log10(tumors.all.resistance.cells))
#d_resistance = density(log10(tumors.all.resistance.cells))
plot(d,xaxt="n",xlim=c(1,10),main="",ylab="Density",xlab="Subclone size (number of cells)",cex.main=1.8,cex.lab=1.5,cex = 1.5,cex.axis=1.6, col=rgb(0,0,1,0.7))
#plot(range(d$x, d$x), range(d$y, d$y), type = "n")
#lines(d,col=rgb(0,0,1,0.7))
#lines(d_resistance,col=rgb(1,0,0,0.6))
polygon(d, col=rgb(0,0,1,0.7))
#polygon(d_resistance, col=rgb(1,0,0,0.6))
ticks <- seq(2, 10, by=2)
labels <- sapply(ticks, function(i) as.expression(bquote(10^.(i))))
axis(1, at=c(2,4,6,8,10), labels=labels, cex.axis=1.6)


####BD9

#tumors.all.cells=vector()

tumors.all.resistance.cells=vector()

for (i in 1:nrow(tumors.ids.BD9)){
  
  tumors.BD9 = subset(tumors.all.BD9, (tumors.all.BD9$tumorid == tumors.ids.BD9[i,]) & (tumors.all.BD9$cells>=min.size.subclones) & (tumors.all.BD9$mutations>0))
  
  tumors.BD9.resistance=subset(tumors.BD9,tumors.BD9$mutation_selection<1e-08)
  
  #tumors.all.cells=c(tumors.all.cells,tumors.BD9$cells)
  
  tumors.all.resistance.cells=c(tumors.all.resistance.cells,tumors.BD9.resistance$cells)
  
}

##Density plot
d <- density(log10(tumors.all.resistance.cells))
#d_resistance = density(log10(tumors.all.resistance.cells))
plot(d,xaxt="n",xlim=c(1,10),main="",ylab="Density",xlab="Subclone size (number of cells)",cex.main=1.8,cex.lab=1.5,cex = 1.5,cex.axis=1.6, col=rgb(0,0,1,0.7))
#plot(range(d$x, d$x), range(d$y, d$y), type = "n")
#lines(d,col=rgb(0,0,1,0.7))
#lines(d_resistance,col=rgb(1,0,0,0.6))
polygon(d, col=rgb(0,0,1,0.7))
#polygon(d_resistance, col=rgb(1,0,0,0.6))
ticks <- seq(2, 10, by=2)
labels <- sapply(ticks, function(i) as.expression(bquote(10^.(i))))
axis(1, at=c(2,4,6,8,10), labels=labels, cex.axis=1.6)


####CC9

#tumors.all.cells=vector()

tumors.all.resistance.cells=vector()

for (i in 1:nrow(tumors.ids.CC9)){
  
  tumors.CC9 = subset(tumors.all.CC9, (tumors.all.CC9$tumorid == tumors.ids.CC9[i,]) & (tumors.all.CC9$cells>=min.size.subclones) & (tumors.all.CC9$mutations>0))
  
  tumors.CC9.resistance=subset(tumors.CC9,tumors.CC9$mutation_selection<1e-08)
  
  #tumors.all.cells=c(tumors.all.cells,tumors.AD9$cells)
  
  tumors.all.resistance.cells=c(tumors.all.resistance.cells,tumors.CC9.resistance$cells)
  
}

##Density plot
d <- density(log10(tumors.all.resistance.cells))
#d_resistance = density(log10(tumors.all.resistance.cells))
plot(d,xaxt="n",xlim=c(1,10),main="",ylab="Density",xlab="Subclone size (number of cells)",cex.main=1.8,cex.lab=1.5,cex = 1.5,cex.axis=1.6, col=rgb(0,0,1,0.7))
#plot(range(d$x, d$x), range(d$y, d$y), type = "n")
#lines(d,col=rgb(0,0,1,0.7))
#lines(d_resistance,col=rgb(1,0,0,0.6))
polygon(d, col=rgb(0,0,1,0.7))
#polygon(d_resistance, col=rgb(1,0,0,0.6))
ticks <- seq(2, 10, by=2)
labels <- sapply(ticks, function(i) as.expression(bquote(10^.(i))))
axis(1, at=c(2,4,6,8,10), labels=labels, cex.axis=1.6)


####CD9

#tumors.all.cells=vector()

tumors.all.resistance.cells=vector()

for (i in 1:nrow(tumors.ids.CD9)){
  
  tumors.CD9 = subset(tumors.all.CD9, (tumors.all.CD9$tumorid == tumors.ids.CD9[i,]) & (tumors.all.CD9$cells>=min.size.subclones) & (tumors.all.CD9$mutations>0))
  
  tumors.CD9.resistance=subset(tumors.CD9,tumors.CD9$mutation_selection<1e-08)
  
  #tumors.all.cells=c(tumors.all.cells,tumors.CD9$cells)
  
  tumors.all.resistance.cells=c(tumors.all.resistance.cells,tumors.CD9.resistance$cells)
  
}

##Density plot
d <- density(log10(tumors.all.resistance.cells))
#d_resistance = density(log10(tumors.all.resistance.cells))
plot(d,xaxt="n",xlim=c(1,10),main="",ylab="Density",xlab="Subclone size (number of cells)",cex.main=1.8,cex.lab=1.5,cex = 1.5,cex.axis=1.6, col=rgb(0,0,1,0.7))
#plot(range(d$x, d$x), range(d$y, d$y), type = "n")
#lines(d,col=rgb(0,0,1,0.7))
#lines(d_resistance,col=rgb(1,0,0,0.6))
polygon(d, col=rgb(0,0,1,0.7))
#polygon(d_resistance, col=rgb(1,0,0,0.6))
ticks <- seq(2, 10, by=2)
labels <- sapply(ticks, function(i) as.expression(bquote(10^.(i))))
axis(1, at=c(2,4,6,8,10), labels=labels, cex.axis=1.6)

par(old.par)
