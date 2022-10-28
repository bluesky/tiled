
import Auth from '../components/auth';
import { useParams } from "react-router-dom";

/*
public
single user api key
login providers
    Oauth2
    Basic
*/

const Login = () => {
    return (
        // list of buttons of providers to choose from
        <Auth/>
    );
}

export default Login
