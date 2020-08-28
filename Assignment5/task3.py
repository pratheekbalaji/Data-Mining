def fit(self, data_points, dic):
    self.centroids = {}
    # random.seed(5)
    cp = random.sample(data_points, self.k)
    self.centroids.append(random.sample(data_points, 1))

    for a in range(self.k - 1):
        dist = []

        for p in data_points:
            d = sys.maxsize
            points = data_points[p]
            for c in range(len(self.centroids)):
                temp_dist = math.sqrt(math.sqrt(sum([(i - j) ** 2 for i, j in zip(self.centorids[c], points)]))
                d = min(d, temp_dist)
            dist.append(d)
        next_centroid = max(dist)
        self.centroids.append(next_centroid)
        dist = []