Spark Star Join
====

[![DOI](https://zenodo.org/badge/doi/10.5281/zenodo.19154.svg)](http://dx.doi.org/10.5281/zenodo.19154)


You will find in this repository the implementation of two efficient solutions for Star Joins using Spark framework, dropping the computation time by at least 60% when compared to other solutions available. Namely, they are the Spark Bloom-Filtered Cascade Join (SBFCJ) and the Spark Broadcast Join (SBJ). Each of these strategies excel in different scenarios ([for more details, click here](http://www.sciencedirect.com/science/article/pii/S1877050916306500)): SBJ is twice faster when the memory available to each executor is large enough; and SBFCJ is remarkably resilient to low memory scenarios. As of now, these algorithms are very competitive, and may be easily combined with other technologies for further improvement (such as new data types or file managements).

You will also find a direct Spark implementation of a sequence of joins, which delivers very poor performance and is far from being eligible as a good solution. This shows the importance of additional filtering. 

These strategies were presented and studied in our recent paper, [Brito et al, "*Faster Cloud Star Joins with Reduced Disk Spill and Network Communication*"](http://www.sciencedirect.com/science/article/pii/S1877050916306500). If you find it useful for your own research/applications, please cite our work and/or star our repository. If you need more information or have suggestions, feel free to either contact me or make a PR. Feedback is very important.


Acknowledgements
---

We acknowledge Microsoft Azure Research grant MS-AZR-0036P, FAPESP grant 2012/13158-9 and CNPq grant 234817/2014-3.


License
---

Feel free to use this code for studying, applying to your own problems, or anything that complies with the MIT License (MIT), available in the folder License. If you use this code, we kindly ask that you cite our paper [Brito et al](http://www.sciencedirect.com/science/article/pii/S1877050916306500), where both of these strategies were presented and studied. 
