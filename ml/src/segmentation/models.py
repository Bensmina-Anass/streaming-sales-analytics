from sklearn.cluster import KMeans


def get_segmentation_models(random_state: int = 42) -> dict:
    return {
        "kmeans_3": KMeans(n_clusters=3, random_state=random_state, n_init=10),
        "kmeans_4": KMeans(n_clusters=4, random_state=random_state, n_init=10),
        "kmeans_5": KMeans(n_clusters=5, random_state=random_state, n_init=10),
        "kmeans_6": KMeans(n_clusters=6, random_state=random_state, n_init=10),
    }