namespace naivedb.core.storage
{
    /// <summary>
    /// Represents an interface for a storage mechanism allowing the addition of records and retrieval of all stored records.
    /// </summary>
    public interface IStorage
    {
        /// <summary>
        /// Adds a new record to the storage mechanism.
        /// </summary>
        /// <param name="record">The record to append to the storage.</param>
        void Append(Record record);

        /// <summary>
        /// Retrieves all records from the storage mechanism.
        /// </summary>
        /// <returns>An enumerable collection of all stored records.</returns>
        IEnumerable<Record> ReadAll();

        /// <summary>
        /// Saves all the specified records to the storage mechanism, replacing existing stored records.
        /// </summary>
        /// <param name="records">The list of records to save to the storage.</param>
        /// <param name="lastOperation">Last operation name. Default is update.</param>
        void SaveAll(List<Record> records, string lastOperation = "update");

        /// <summary>
        /// Retrieves metadata information about a page in the storage mechanism.
        /// </summary>
        /// <returns>An object containing metadata details for the page.</returns>
        PageMetadata GetMetadata();
    }
}