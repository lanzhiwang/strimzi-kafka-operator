package io.strimzi.controller.cluster.resources;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.vertx.core.json.JsonObject;

import java.util.Map;
import java.util.stream.Collectors;

public class Storage {

    public static final String TYPE_FIELD = "type";
    public static final String SIZE_FIELD = "size";
    public static final String STORAGE_CLASS_FIELD = "class";
    public static final String SELECTOR_FIELD = "selector";
    public static final String SELECTOR_MATCH_LABELS_FIELD = "match-labels";
    public static final String DELETE_CLAIM_FIELD = "delete-claim";

    private final StorageType type;
    private Quantity size;
    private String storageClass;
    private LabelSelector selector;
    private boolean isDeleteClaim = false;

    public enum StorageType {

        EPHEMERAL("ephemeral"),
        PERSISTENT_CLAIM("persistent-claim"),
        LOCAL("local");

        private final String type;

        private StorageType(String type) {
            this.type = type;
        }

        public static StorageType from(String type) {
            if (type.equals(EPHEMERAL.type)) {
                return EPHEMERAL;
            } else if (type.equals(PERSISTENT_CLAIM.type)) {
                return PERSISTENT_CLAIM;
            } else if (type.equals(LOCAL.type)) {
                return LOCAL;
            } else {
                throw new IllegalArgumentException("Unknown type: " + type);
            }
        }
    }

    public static class StorageDiffResult {

        private boolean isType;
        private boolean isSize;
        private boolean isStorageClass;
        private boolean isSelector;
        private boolean isDeleteClaim;

        public boolean isType() {
            return this.isType;
        }

        public boolean isSize() {
            return this.isSize;
        }

        public boolean isStorageClass() {
            return this.isStorageClass;
        }

        public boolean isSelector() {
            return this.isSelector;
        }

        public boolean isDeleteClaim() {
            return this.isDeleteClaim;
        }

        public StorageDiffResult withDifferentType(boolean isType) {
            this.isType = isType;
            return this;
        }

        public StorageDiffResult withDifferentSize(boolean isSize) {
            this.isSize = isSize;
            return this;
        }

        public StorageDiffResult withDifferentStorageClass(boolean isStorageClass) {
            this.isStorageClass = isStorageClass;
            return this;
        }

        public StorageDiffResult withDifferentSelector(boolean isSelector) {
            this.isSelector = isSelector;
            return this;
        }

        public StorageDiffResult withDifferentDeleteClaim(boolean isDeleteClaim) {
            this.isDeleteClaim = isDeleteClaim;
            return this;
        }
    }

    public static Storage fromJson(JsonObject json) {
        // Storage.TYPE_FIELD = type
        String type = json.getString(Storage.TYPE_FIELD);
        if (type == null) {
            throw new IllegalArgumentException("Storage '" + Storage.TYPE_FIELD + "' is mandatory");
        }

        Storage storage = new Storage(StorageType.from(type));

        // Storage.SIZE_FIELD = size
        String size = json.getString(Storage.SIZE_FIELD);
        if (size != null) {
            storage.withSize(new Quantity(size));
        }

        // Storage.STORAGE_CLASS_FIELD = class
        String storageClass = json.getString(Storage.STORAGE_CLASS_FIELD);
        if (storageClass != null) {
            storage.withClass(storageClass);
        }

        // Storage.DELETE_CLAIM_FIELD = delete-claim
        if (json.getValue(Storage.DELETE_CLAIM_FIELD) instanceof Boolean) {
            boolean isDeleteClaim = json.getBoolean(Storage.DELETE_CLAIM_FIELD);
            storage.withDeleteClaim(isDeleteClaim);
        }

        // Storage.SELECTOR_FIELD = selector
        JsonObject selector = json.getJsonObject(Storage.SELECTOR_FIELD);
        if (selector != null) {
            // Storage.SELECTOR_MATCH_LABELS_FIELD = match-labels
            JsonObject matchLabelsJson = selector.getJsonObject(Storage.SELECTOR_MATCH_LABELS_FIELD);
            Map<String, String> matchLabels =
                    matchLabelsJson.getMap().entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));
            // match-expressions doesn't supported yet, so null first argument
            storage.withSelector(new LabelSelector(null, matchLabels));
        }

        return storage;
    }

    public static Storage fromPersistentVolumeClaim(PersistentVolumeClaim pvc) {
        Storage storage = new Storage(StorageType.PERSISTENT_CLAIM);
        storage.withSize(pvc.getSpec().getResources().getRequests().get("storage"))
                .withClass(pvc.getSpec().getStorageClassName());

        if (pvc.getSpec().getSelector() != null) {
            storage.withSelector(pvc.getSpec().getSelector());
        }

        return storage;
    }

    public Storage(StorageType type) {
        this.type = type;
    }

    public Storage withSize(final Quantity size) {
        this.size = size;
        return this;
    }

    public Storage withClass(final String storageClass) {
        this.storageClass = storageClass;
        return this;
    }

    public Storage withSelector(final LabelSelector selector) {
        this.selector = selector;
        return this;
    }

    public Storage withDeleteClaim(final boolean isDeleteClaim) {
        this.isDeleteClaim = isDeleteClaim;
        return this;
    }

    public StorageType type() {
        return this.type;
    }

    public Quantity size() {
        return this.size;
    }

    public String storageClass() {
        return this.storageClass;
    }

    public LabelSelector selector() {
        return this.selector;
    }

    public boolean isDeleteClaim() {
        return this.isDeleteClaim;
    }

    public StorageDiffResult diff(Storage other) {

        StorageDiffResult diffResult = new StorageDiffResult();

        diffResult
                .withDifferentType(this.type != other.type())
                .withDifferentSize(!this.compareSize(other.size()))
                .withDifferentDeleteClaim(this.isDeleteClaim != other.isDeleteClaim())
                .withDifferentStorageClass(!this.compareStorageClass(other.storageClass()))
                .withDifferentSelector(!this.compareSelector(other.selector()));

        return diffResult;
    }

    private boolean compareSize(Quantity other) {
        return this.size == null ?
                other == null : other != null && this.size.getAmount().equals(other.getAmount());
    }

    private boolean compareStorageClass(String other) {
        return this.storageClass == null ?
                other == null : this.storageClass.equals(other);
    }

    private boolean compareSelector(LabelSelector other) {
        if (this.selector != null && other != null) {

            if (this.selector.getMatchLabels() == null && other.getMatchLabels() != null) {
                return false;
            }

            if (this.selector.getMatchLabels() != null && other.getMatchLabels() == null) {
                return false;
            }

            if (this.selector.getMatchLabels().size() != other.getMatchLabels().size()) {
                return false;
            }

            return this.selector.getMatchLabels().entrySet().equals(other.getMatchLabels().entrySet());

        } else {
            return this.selector == null && other == null;
        }
    }

}
