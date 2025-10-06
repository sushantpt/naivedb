using naivedb.core.storage;

namespace naivedb.core.engine
{
    /// <summary>
    /// Represents an interface for database operations, defining methods for CRUD operations and data retrieval.
    /// </summary>
    public interface IDatabase
    {
        /// <summary>
        /// Adds a new record to the database.
        /// </summary>
        /// <param name="record">The <see cref="Record"/> instance to be added to the database.</param>
        /// /// <param name="tableName">Name of the table.</param>
        void Create(string tableName, Record record);
        
        /// <summary>
        /// Updates an existing record in the database.
        /// </summary>
        /// <param name="record">The <see cref="Record"/> instance containing updated information to be saved to the database.</param>
        /// /// <param name="tableName">Name of the table.</param>
        void Update(string tableName, Record record);

        /// <summary>
        /// Deletes a record from the database based on the specified key.
        /// </summary>
        /// <param name="key">The unique identifier of the record to be deleted.</param>
        /// /// <param name="tableName">Name of the table.</param>
        void Delete(string tableName, string key);

        /// <summary>
        /// Retrieves all records from the specified table.
        /// </summary>
        /// <param name="tableName">Name of the table to read records from.</param>
        /// <returns>A <see cref="ResultSet"/> instance containing all records from the specified table.</returns>
        ResultSet ReadAll(string tableName);

        /// <summary>
        /// Retrieves metadata associated with the specified table.
        /// </summary>
        /// <param name="tableName">The name of the table for which metadata is to be retrieved.</param>
        /// <returns>A <see cref="PageMetadata"/> instance containing metadata for the specified table.</returns>
        PageMetadata GetTableMetadata(string tableName);
    }
}