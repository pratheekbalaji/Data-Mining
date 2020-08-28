def merge_clusters(cluster_1stats, cluster_2stats, cluster_1points, cluster_2points, kind):
    alpha = 2
    cluster_2points_copy = cluster_2points.copy()
    for idx_1, c in enumerate(cluster_2stats):
        candidates = []

        flag = False
        n1 = c[0]
        c1_sum = c[1]

        for idx, d in enumerate(cluster_1stats):
            dis = []
            n2 = d[0]
            c2_sum = d[1]

            c2_sum_sq = d[2]
            for a, b in enumerate(c2_sum):
                point = c1_sum[a] / n1

                ci = b / n2
                # print(n)

                di = point - ci

                # print(sumsq[a])

                var = (c2_sum_sq[a] / n2 - (ci ** 2))

                if var == 0:
                    continue
                # print(variance)
                t = ((di / var) ** 2)

                dis.append(t)

            r = sum(dis)
            r = math.sqrt(r)

            threshold = math.sqrt(len(c2_sum))

            if (r <= alpha * threshold):
                candidates.append((idx, r))

        if len(candidates) > 0:
            candidates = sorted(candidates, key=lambda x: x[1])[0]
            a, b = candidates[0], candidates[1]
            s = cluster_2points[idx_1]

            cluster_1points[a].extend(s)
            del cluster_2points_copy[idx_1]
            flag = True

        if flag == False and kind == 'intermediate':
            k = len(cluster_1points)
            cluster_1points[k] = []
            s = cluster_2points[idx_1]

            cluster_1points[k].extend(s)
            del cluster_2points_copy[idx_1]

    if kind == 'intermediate':
        return cluster_1points

    if kind =='final':
        return cluster_1points,cluster_2points_copy
