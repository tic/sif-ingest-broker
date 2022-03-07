// Load the broker configuration
const { config } = require("../config");


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
async function validateCognitoAuthToken(token) {
    const { error, user } = await new Promise((resolve, _) => {
        CognitoExpress.validate(token, (err, user) => {
            if (err) {
                // Optional code which could, eventually, be used to
                // check if an access token was accidentially provided
                // and, if so, verify that and use the resulting info
                // to either report an error to the error log or just
                // to proceed with the username found in that token.
                // var base64Url = token.split('.')[1];
                // var base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
                // var jsonPayload = 
                //     decodeURIComponent(
                //         Buffer.from(
                //             base64, 
                //             'base64'
                //         )
                //         .toString()
                //         .split('')
                //         .map(function(c) {
                //             return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
                //         })
                //         .join(''));
                // console.log(jsonPayload);

                resolve({
                    error: err,
                    user: null
                });
            }
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
    validateToken: validateCognitoAuthToken
};
