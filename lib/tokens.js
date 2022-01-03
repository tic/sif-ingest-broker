// Load the broker configuration
const { config } = require('./config');


// Initialize the AWS Cognito connector
const CognitoExpress = new (require("cognito-express"))({
    region: config.REGION,
    cognitoUserPoolId: config.USERPOOLID,
    tokenUse: "id",
    tokenExpiration: 3600000
});


// Given a JWT (JSON Web Token) for AWS Cognito,
// the function returns the following object:
//
// @returns {
//     success: boolean --> was token valid?
//     username: string --> username of the user who owns the provided token, if valid
//     groups: array[string] --> array of groups which the user is a part of
//     error: string --> if an error is encountered, it will be returned here
// }
async function validateAuthToken(token) {
    const { error, user } = await new Promise((resolve, _) => {
        CognitoExpress.validate(token, (err, user) => {
            if (err) resolve({
                error: err,
                user: null
            });
            else if (!user) resolve({
                error: "Could not parse valid user from token",
                user: null
            });
            else resolve({
                error: null,
                user: user
            })
        });
    });

    return {
        success: !error,
        username: user ? (user["cognito:username"] || user.username) : null,
        groups: user ? user["cognito:groups"] : [],
        error: error
    }
}

module.exports = {
    validate: validateAuthToken
};
