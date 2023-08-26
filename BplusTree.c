

typedef unsigned char uint8_t;
typedef unsigned int uint32_t;

/* B+ tree节点 内部节点 叶子节点*/
typedef enum {
    NODE_INTERNAL,
    NODE_LEAF,
} NodeType;

/**
 * Common Node Header Layout
*/
#define NODE_TYPE_SIZE sizeof(uint8_t)
#define NODE_TYPE_OFFSET 0
#define IS_ROOT_SIZE sizeof(uint8_t)
#define IS_ROOT_OFFSET NODE_TYPE_SIZE
#define PARENT_POINTER_SIZE sizeof(uint32_t)
#define PARENT_POINTER_OFFSET (IS_ROOT_SIZE + NODE_TYPE_SIZE)
#define COMMON_NODE_SIZE (NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE)

/**
 * Leaf Node Header Layout 叶子头节点其他信息
*/
#define LEAF_NODE_NUM_CELLS_SIZE sizeof(uint32_t)
#define LEAF_NODE_NUM_CELLS_OFFSET COMMON_NODE_SIZE
#define LEAF_NODE_HEADER_SIZE (COMMON_NODE_SIZE + LEAF_NODE_NUM_CELLS_SIZE)

/**
 * Leaf Node Body Layout 叶子节点键/值信息
*/
#define LEAF_NODE_KEY_SIZE sizeof(uint32_t)
#define LEAF_NODE_KEY_OFFSET 0
#define LEAF_NODE_VALUE_SIZE ROW_SIZE
#define LEAF_NODE_VALUE_OFFSET (LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE)
#define LEAF_NODE_CELL_SIZE (LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE)
#define LEAF_NODE_SPACE_FOR_CELLS (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)
#define LEAF_NODE_MAX_CELLS (LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_NUM_CELLS_SIZE)