/* global use, db */
// MongoDB Playground
// To disable this template go to Settings | MongoDB | Use Default Template For Playground.
// Make sure you are connected to enable completions and to be able to run a playground.
// Use Ctrl+Space inside a snippet or a string literal to trigger completions.
// The result of the last command run in a playground is shown on the results panel.
// By default the first 20 documents will be returned with a cursor.
// Use 'console.log()' to print to the debug output.
// For more documentation on playgrounds please refer to
// https://www.mongodb.com/docs/mongodb-vscode/playgrounds/

// Select the database to use.
use('csci');

// #RELEASE YEAR 
db.getCollection('project').aggregate(
    [
      {
        $match: { release_year: { $exists: true } }
      },
      {
        $group: {
          _id: '$release_year',
          count: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ],
    { maxTimeMS: 60000, allowDiskUse: true }
  );

// #top genres by country 
db.getCollection('project').aggregate([
    { $unwind: "$listed_in" },
    { $group: { _id: { country: "$country", genre: "$listed_in" }, count: { $sum: 1 } } },
    { $sort: { "_id.country": 1, count: -1 } },
    { $group: { _id: "$_id.country", top_genres: { $push: { genre: "$_id.genre", count: "$count" } } } },
    { $project: { country: "$_id", top_genres: { $slice: ["$top_genres", 3] } } }
]);

// // #top rate similar shows 
// db.getCollection('project').aggregate([
//     // Stage 1: Filter by rating
//     {
//         $match: { 
//             rating: { $in: ["PG", "PG-13", "G"] } // Adjust ratings as needed
//         }
//     },
//     // Stage 2: Extract numeric duration from the "duration" field
//     {
//         $addFields: {
//             numeric_duration: {
//                 $toInt: {
//                     $substr: ["$duration", 0, { $indexOfBytes: ["$duration", " "] }]
//                 }
//             }
//         }
//     },
//     // Stage 3: Lookup similar shows
//     {
//         $lookup: {
//             from: "project", // Same collection
//             let: {
//                 currentShowId: "$show_id",
//                 currentType: "$type",
//                 currentCountry: "$country",
//                 currentDuration: "$numeric_duration"
//             },
//             pipeline: [
//                 {
//                     $match: {
//                         $expr: {
//                             $and: [
//                                 { $ne: ["$show_id", "$$currentShowId"] }, // Exclude the current show
//                                 { $eq: ["$type", "$$currentType"] }, // Match by type
//                                 { $eq: ["$country", "$$currentCountry"] }, // Match by country
//                                 { $lte: [{ $abs: { $subtract: ["$numeric_duration", "$$currentDuration"] } }, 15] } // Similar duration ±15 mins
//                             ]
//                         }
//                     }
//                 }
//             ],
//             as: "similar_shows"
//         }
//     },
//     // Stage 4: Project the required fields and limit similar_shows to 10
//     {
//         $project: {
//             show_id: 1,
//             title: 1,
//             rating: 1,
//             type: 1,
//             country: 1,
//             numeric_duration: 1,
//             similar_shows: { $slice: ["$similar_shows", 10] } // Limit to 10 similar shows
//         }
//     },
//     // Stage 5: Sort by rating
//     {
//         $sort: { rating: -1 } // Adjust the sort field if needed
//     },
//     // Stage 6: Limit the results
//     {
//         $limit: 100 // Limit to top 100 shows
//     }
// ], 
// // Enable disk use for sorting bc mongodb allows 30 Mb reading only 
// { allowDiskUse: true });


// // # genre popularity by month of release
// db.getCollection('project').aggregate([
//     // Stage 1: Convert date_added from string to Date
//     {
//         $addFields: {
//             date_added: {
//                 $dateFromString: { dateString: "$date_added" }
//             }
//         }
//     },
//     // Stage 2: Filter documents where listed_in exists
//     {
//         $match: { listed_in: { $exists: true } }
//     },
//     // Stage 3: Unwind the genres into separate documents
//     {
//         $unwind: "$listed_in"
//     },
//     // Stage 4: Group by month and genre, and count occurrences
//     {
//         $group: {
//             _id: { 
//                 month: { $month: "$date_added" }, // Extract month from date_added
//                 genre: "$listed_in" // Group by genre
//             },
//             count: { $sum: 1 } // Count occurrences
//         }
//     },
//     // Stage 5: Sort by popularity (count) in descending order
//     {
//         $sort: { count: -1 }
//     }
// ]);

// #top monthlyrelease 
// db.getCollection('project').aggregate([
//     // Stage 1: Convert date_added from string to Date
//     {
//         $addFields: {
//             date_added: {
//                 $dateFromString: { dateString: "$date_added" }
//             }
//         }
//     },
//     // Stage 2: Group by month and content type, and count occurrences
//     {
//         $group: {
//             _id: { 
//                 month: { $month: "$date_added" },
//                 type: "$type"
//             },
//             count: { $sum: 1 } // Count the number of releases
//         }
//     },
//     // Stage 3: Sort by count in descending order
//     {
//         $sort: { count: -1 }
//     }
// ]);



// // #genre trends by country and year 

// db.getCollection('project').aggregate([
//     // Stage 1: Filter out documents with null values
//     {
//         $match: { 
//             country: { $ne: null },
//             listed_in: { $ne: null },
//             release_year: { $ne: null }
//         }
//     },
//     // Stage 2: Split "listed_in" into an array of genres
//     {
//         $addFields: {
//             genres: { $split: ["$listed_in", ", "] } // Splits by comma and space
//         }
//     },
//     // Stage 3: Unwind the genres array
//     {
//         $unwind: "$genres"
//     },
//     // Stage 4: Group by country, release_year, and genre, and count titles
//     {
//         $group: {
//             _id: { 
//                 country: "$country", 
//                 release_year: "$release_year", 
//                 genre: "$genres"
//             },
//             title_count: { $sum: 1 } // Count the titles
//         }
//     },
//     // Stage 5: Sort the results
//     {
//         $sort: { 
//             "_id.country": 1, 
//             "_id.release_year": 1, 
//             "_id.genre": 1 
//         }
//     }
// ]);



// // #Most Popular Ratings by Year
// db.getCollection('project').aggregate([
//     // Stage 1: Group by release_year and rating, and count films
//     {
//         $group: {
//             _id: {
//                 release_year: "$release_year", 
//                 rating: "$rating"
//             },
//             count: { $sum: 1 } // Count the number of films
//         }
//     },
//     // Stage 2: Sort by count in descending order
//     {
//         $sort: {
//             count: -1
//         }
//     }
// ]);