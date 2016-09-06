#This script generates the plot of the correlation between the average number of dominant clones and the average number of minors subclones
# Some of the outputs generated from the script 'Intratumor_Subclonal_Diversity.R' are used in this script

plot(avg.subclones.g10frequency.all.tumors,avg.subclones.l10frequency.all.tumors,xlab="Average number of dominant subclones", ylab="Log10 average number of minor subclones", main="", pch=16, cex.main=1.8, cex.lab=1.7, cex = 2.5, cex.axis=2)
fit=lm(avg.subclones.l10frequency.all.tumors ~ avg.subclones.g10frequency.all.tumors)
abline(fit)
cor.test(avg.subclones.g10frequency.all.tumors, avg.subclones.l10frequency.all.tumors, method="spearman")